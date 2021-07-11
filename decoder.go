// Copyright 2019+ Klaus Post. All rights reserved.
// License information can be found in the LICENSE file.
// Based on work by Yann Collet, released under BSD License.

package zstd

import (
	"io"
)

// Decoder provides decoding of zstandard streams.
// The decoder has been designed to operate without allocations after a warmup.
// This means that you should store the decoder for best performance.
// To re-use a stream decoder, use the Reset(r io.Reader) error to switch to another stream.
// A decoder can safely be re-used even if the previous stream failed.
// To release the resources, you must call the Close() function on a decoder.
type Decoder struct {
	o decoderOptions

	// Unreferenced decoders, ready for use.
	decoders *blockDec

	r      io.Reader
	output []byte

	// Current read position used for Reader functionality.
	current decoderState

	// Custom dictionaries.
	// Always uses copies.
	dicts map[uint32]dict
}

// decoderState is used for maintaining state when the decoder
// is used for streaming.
type decoderState struct {
	// current block being written to stream.
	decodeOutput

	flushed bool
}

var (
	// Check the interfaces we want to support.
	_ = io.WriterTo(&Decoder{})
	_ = io.Reader(&Decoder{})
)

// NewReader creates a new decoder.
// A nil Reader can be provided in which case Reset can be used to start a decode.
//
// A Decoder can be used in two modes:
//
// 1) As a stream, or
// 2) For stateless decoding using DecodeAll.
//
// Only a single stream can be decoded concurrently, but the same decoder
// can run multiple concurrent stateless decodes. It is even possible to
// use stateless decodes while a stream is being decoded.
//
// The Reset function can be used to initiate a new stream, which is will considerably
// reduce the allocations normally caused by NewReader.
func NewReader(r io.Reader, opts ...DOption) (*Decoder, error) {
	initPredefined()
	var d Decoder
	d.o.setDefault()
	for _, o := range opts {
		err := o(&d.o)
		if err != nil {
			return nil, err
		}
	}
	d.current.flushed = true

	if r == nil {
		d.current.err = ErrDecoderNilInput
	}

	// Transfer option dicts.
	d.dicts = make(map[uint32]dict, len(d.o.dicts))
	for _, dc := range d.o.dicts {
		d.dicts[dc.id] = dc
	}
	d.o.dicts = nil

	// Create decoders

	dec := newBlockDec(d.o.lowMem)
	dec.localFrame = newFrameDec(d.o)
	d.decoders = dec
	d.output = make([]byte, 0, 1<<20)
	d.current.b = d.output

	if r == nil {
		return &d, nil
	}

	d.r = r
	return &d, d.Reset(r)
}

// Read bytes from the decompressed stream into p.
// Returns the number of bytes written and any error that occurred.
// When the stream is done, io.EOF will be returned.
func (d *Decoder) Read(p []byte) (int, error) {
	var n int
	for {
		if len(d.current.b) > 0 {
			filled := copy(p, d.current.b)
			p = p[filled:]
			d.current.b = d.current.b[filled:]
			n += filled
		}
		if len(p) == 0 {
			break
		}
		if len(d.current.b) == 0 {
			// We have an error and no more data
			if d.current.err != nil {
				break
			}
			err := d.tryDecompress()
			if err != nil {
				d.current.err = err
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					d.current.err = io.EOF
					if len(d.current.b) != 0 {
						continue
					}
				}
				break
			}
		}
	}
	if len(d.current.b) > 0 {
		if debugDecoder {
			println("returning", n, "still bytes left:", len(d.current.b))
		}
		// Only return error at end of block
		return n, nil
	}
	if d.current.err != nil {
		d.drainOutput()
	}
	if debugDecoder {
		println("returning", n, d.current.err)
	}
	return n, d.current.err
}

// Reset will reset the decoder the supplied stream after the current has finished processing.
// Note that this functionality cannot be used after Close has been called.
// Reset can be called with a nil reader to release references to the previous reader.
// After being called with a nil reader, no other operations than Reset or DecodeAll or Close
// should be used.
func (d *Decoder) Reset(r io.Reader) error {
	if d.current.err == ErrDecoderClosed {
		return d.current.err
	}

	d.drainOutput()

	if r == nil {
		d.current.err = ErrDecoderNilInput
		if len(d.current.b) > 0 {
			d.current.b = d.current.b[:0]
		}
		d.current.flushed = true
		return nil
	}

	// If bytes buffer and < 5MB, do sync decoding anyway.
	if bb, ok := r.(byter); ok && bb.Len() < 5<<20 {
		bb2 := bb
		if debugDecoder {
			println("*bytes.Buffer detected, doing sync decode, len:", bb.Len())
		}
		b := bb2.Bytes()
		var dst []byte
		if cap(d.current.b) > 0 {
			dst = d.current.b
		}

		dst, err := d.DecodeAll(b, dst[:0])
		if err == nil {
			err = io.EOF
		}
		d.current.b = dst
		d.current.err = err
		d.current.flushed = true
		if debugDecoder {
			println("sync decode to", len(dst), "bytes, err:", err)
		}
		return nil
	}

	// Remove current block.
	d.current.decodeOutput = decodeOutput{}
	d.current.err = nil
	d.current.flushed = false
	d.current.d = nil
	d.r = r

	return nil
}

// drainOutput will drain the output until errEndOfStream is sent.
func (d *Decoder) drainOutput() {

	if d.current.d != nil {
		if debugDecoder {
			printf("re-adding current decoder %p", d.current.d)
		}
		d.decoders = d.current.d
		d.current.d = nil
		d.current.b = nil
	}
	if d.current.flushed {
		println("current already flushed")
		return
	}
}

// WriteTo writes data to w until there's no more data to write or when an error occurs.
// The return value n is the number of bytes written.
// Any error encountered during the write is also returned.
func (d *Decoder) WriteTo(w io.Writer) (int64, error) {
	var n int64
	for {
		if len(d.current.b) > 0 {
			n2, err2 := w.Write(d.current.b)
			n += int64(n2)
			d.current.b = d.current.b[n2:]
			if err2 != nil && d.current.err == nil {
				d.current.err = err2
				break
			}
		}
		if d.current.err != nil {
			break
		}

		if len(d.current.b) == 0 {
			err := d.tryDecompress()
			if err != nil {
				d.current.err = err
				break
			}
		}
	}
	err := d.current.err
	if err != nil {
		d.drainOutput()
	}
	if err == io.EOF {
		err = nil
	}
	return n, err
}

func (d *Decoder) tryDecompress() error {
	if d.current.err == ErrDecoderClosed {
		return ErrDecoderClosed
	}

	// Grab a block decoder and frame decoder.
	block := d.decoders
	frame := block.localFrame
	defer func() {
		if debugDecoder {
			printf("re-adding decoder: %p", block)
		}
		frame.rawInput = nil
		frame.bBuf = nil
	}()
	br := readerWrapper{r: d.r}

	dst := d.current.b
	defer func() {
		d.current.b = dst
	}()

	for {
		frame.history.reset()
		err := frame.reset(&br)
		if err == io.EOF {
			if debugDecoder {
				println("frame reset return EOF")
			}
		}
		if frame.DictionaryID != nil {
			dict, ok := d.dicts[*frame.DictionaryID]
			if !ok {
				return ErrUnknownDictionary
			}
			frame.history.setDict(&dict)
		}
		if err != nil {
			return err
		}
		if frame.FrameContentSize > d.o.maxDecodedSize-uint64(len(dst)) {
			return ErrDecoderSizeExceeded
		}
		if frame.FrameContentSize > 0 && frame.FrameContentSize < 1<<30 {
			// Never preallocate moe than 1 GB up front.
			if cap(dst)-len(dst) < int(frame.FrameContentSize) {
				dst2 := make([]byte, len(dst), len(dst)+int(frame.FrameContentSize))
				copy(dst2, dst)
				dst = dst2
			}
		}
		if len(dst) == 0 {
			dst = d.output[:0]
		}

		dst, err = frame.runDecoder(dst, block)
		if err != nil {
			return err
		}
		if len(frame.bBuf) == 0 {
			if debugDecoder {
				println("frame dbuf empty")
			}
			break
		}
	}
	return nil
}

// DecodeAll allows stateless decoding of a blob of bytes.
// Output will be appended to dst, so if the destination size is known
// you can pre-allocate the destination slice to avoid allocations.
// DecodeAll can be used concurrently.
// The Decoder concurrency limits will be respected.
func (d *Decoder) DecodeAll(input, dst []byte) ([]byte, error) {
	if d.current.err == ErrDecoderClosed {
		return dst, ErrDecoderClosed
	}

	// Grab a block decoder and frame decoder.
	block := d.decoders
	frame := block.localFrame
	defer func() {
		if debugDecoder {
			printf("re-adding decoder: %p", block)
		}
		frame.rawInput = nil
		frame.bBuf = nil
	}()
	frame.bBuf = input

	for {
		frame.history.reset()
		err := frame.reset(&frame.bBuf)
		if err == io.EOF {
			if debugDecoder {
				println("frame reset return EOF")
			}
			return dst, nil
		}
		if frame.DictionaryID != nil {
			dict, ok := d.dicts[*frame.DictionaryID]
			if !ok {
				return nil, ErrUnknownDictionary
			}
			frame.history.setDict(&dict)
		}
		if err != nil {
			return dst, err
		}
		if frame.FrameContentSize > d.o.maxDecodedSize-uint64(len(dst)) {
			return dst, ErrDecoderSizeExceeded
		}
		if frame.FrameContentSize > 0 && frame.FrameContentSize < 1<<30 {
			// Never preallocate moe than 1 GB up front.
			if cap(dst)-len(dst) < int(frame.FrameContentSize) {
				dst2 := make([]byte, len(dst), len(dst)+int(frame.FrameContentSize))
				copy(dst2, dst)
				dst = dst2
			}
		}
		if cap(dst) == 0 {
			// Allocate len(input) * 2 by default if nothing is provided
			// and we didn't get frame content size.
			size := len(input) * 2
			// Cap to 1 MB.
			if size > 1<<20 {
				size = 1 << 20
			}
			if uint64(size) > d.o.maxDecodedSize {
				size = int(d.o.maxDecodedSize)
			}
			dst = make([]byte, 0, size)
		}

		dst, err = frame.runDecoder(dst, block)
		if err != nil {
			return dst, err
		}
		if len(frame.bBuf) == 0 {
			if debugDecoder {
				println("frame dbuf empty")
			}
			break
		}
	}
	return dst, nil
}

// Close will release all resources.
// It is NOT possible to reuse the decoder after this.
func (d *Decoder) Close() {
	if d.current.err == ErrDecoderClosed {
		return
	}
	d.drainOutput()

	if d.decoders != nil {
		d.decoders = nil
	}
	if d.current.d != nil {
		d.current.d = nil
	}
	d.current.err = ErrDecoderClosed
}

// IOReadCloser returns the decoder as an io.ReadCloser for convenience.
// Any changes to the decoder will be reflected, so the returned ReadCloser
// can be reused along with the decoder.
// io.WriterTo is also supported by the returned ReadCloser.
func (d *Decoder) IOReadCloser() io.ReadCloser {
	return closeWrapper{d: d}
}

// closeWrapper wraps a function call as a closer.
type closeWrapper struct {
	d *Decoder
}

// WriteTo forwards WriteTo calls to the decoder.
func (c closeWrapper) WriteTo(w io.Writer) (n int64, err error) {
	return c.d.WriteTo(w)
}

// Read forwards read calls to the decoder.
func (c closeWrapper) Read(p []byte) (n int, err error) {
	return c.d.Read(p)
}

// Close closes the decoder.
func (c closeWrapper) Close() error {
	c.d.Close()
	return nil
}

type decodeOutput struct {
	d   *blockDec
	b   []byte
	err error
}
