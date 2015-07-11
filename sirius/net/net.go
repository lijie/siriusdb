package net

import (
	proto "code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"errors"
	"io"
)

var ErrEmptyBody = errors.New("no body")
var ErrRecvBufferSmall = errors.New("recv buffer is too small")

type Header struct {
	Cmd   uint32
	Len   uint32
	Seq   uint32
	Magic uint16
	Err   uint16
}

const (
	maxRecvPacketSize = 64 * 1024
)

func (header *Header) Decode(b []byte) error {
	if len(b) < 16 {
		return io.ErrShortBuffer
	}

	//size := binary.LittleEndian.Uint32(b[4:8])
	//if len(b) < int(size) {
	//	return io.ErrShortBuffer
	//}

	header.Cmd = binary.LittleEndian.Uint32(b[0:4])
	header.Len = binary.LittleEndian.Uint32(b[4:8])
	header.Seq = binary.LittleEndian.Uint32(b[8:12])
	header.Magic = binary.LittleEndian.Uint16(b[12:14])
	header.Err = binary.LittleEndian.Uint16(b[14:16])

	return nil
}

func (header *Header) Encode(b []byte) error {
	if len(b) < 16 {
		return io.ErrShortBuffer
	}
	binary.LittleEndian.PutUint32(b[0:4], header.Cmd)
	binary.LittleEndian.PutUint32(b[4:8], header.Len)
	binary.LittleEndian.PutUint32(b[8:12], header.Seq)
	binary.LittleEndian.PutUint16(b[12:14], header.Magic)
	binary.LittleEndian.PutUint16(b[14:16], header.Err)
	return nil
}

func SendMsg(writer io.Writer, msg proto.Message, cmd uint32, seq uint32) error {
	header := &Header{
		Cmd:   cmd,
		Seq:   seq,
		Magic: 53556,
		Err:   0,
	}
	b, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	var headerbuf [16]byte
	header.Len = 16 + uint32(len(b))
	header.Encode(headerbuf[0:])

	if _, err = writer.Write(headerbuf[0:]); err != nil {
		return err
	}
	if _, err = writer.Write(b); err != nil {
		return err
	}
	return nil
}

// RecvMsg read one message from reader, save header in h
// and unmarshal the body to a msg
func RecvMsg(reader io.Reader, h *Header, msg proto.Message) error {
	var b [maxRecvPacketSize]byte
	n, err := Recv(reader, h, b[0:])
	if err != nil {
		return err
	}
	if n <= 16 {
		// return ErrEmptyBody
		return nil
	}
	err = proto.Unmarshal(b[0:n], msg)
	return err
}

// Recv read one message from reader, save header in h
// and return the body size
func Recv(conn io.Reader, h *Header, b []byte) (int, error) {
	n, err := io.ReadFull(conn, b[0:16])
	if n < 16 || err != nil {
		return 0, err
	}
	err = h.Decode(b[0:16])
	if err != nil {
		return 0, err
	}
	if h.Len <= 16 {
		return 0, nil
	}
	if int(h.Len-16) > len(b) {
		return 0, ErrRecvBufferSmall
	}
	_, err = io.ReadFull(conn, b[0:h.Len-16])
	return int(h.Len - 16), nil
}

// A Buffer is a buffer manager for marshaling and unmarshaling protobuf
// and sirius net header. It may be reused between invocations to reduce
// memory usage.
type Buffer struct {
	Header      Header
	rw          io.ReadWriter
	protoBuffer *proto.Buffer
	buf         [maxRecvPacketSize]byte
	index       int
}

func NewBuffer(rw io.ReadWriter) *Buffer {
	return &Buffer{
		rw:          rw,
		protoBuffer: proto.NewBuffer(nil),
		index:       0,
	}
}

func (b *Buffer) WriteMsg(pb proto.Message, cmd uint32, seq uint32) error {
	b.Header = Header{
		Cmd:   cmd,
		Seq:   seq,
		Magic: 53556,
		Err:   0,
	}
	// we assume that, go will not shrink the slice automatic
	if cap(b.buf[0:0]) != maxRecvPacketSize {
		panic("Go slice memory model has changed !!!???")
	}
	// keep 16 bytes for header,
	// and reset place to intead of proto.Buffer's internal buf
	b.protoBuffer.SetBuf(b.buf[16:16])
	err := b.protoBuffer.Marshal(pb)
	if err != nil {
		return err
	}

	outb := b.protoBuffer.Bytes()
	b.Header.Len = 16 + uint32(len(outb))
	b.Header.Encode(b.buf[0:])
	// fmt.Printf("write %d %d bytes %p %p\n", b.Header.Len, len(outb), &b.buf[0], &outb[0])

	// that's we expect
	if b.Header.Len < maxRecvPacketSize {
		_, err = b.rw.Write(b.buf[0:b.Header.Len])
		return err
	}

	// pb marshaled size is bigger than 64K
	// that meas protoBuffer grows the internal and has a new buf to instead b.buf

	// write header
	if _, err = b.rw.Write(b.buf[0:16]); err != nil {
		return err
	}
	// write body
	if _, err = b.rw.Write(outb); err != nil {
		return err
	}
	return nil
}

func (b *Buffer) ReadMsg(pb proto.Message) error {
	b.Header.Err = 0
	n, err := Recv(b.rw, &b.Header, b.buf[0:])
	if err != nil {
		return err
	}
	if n == 0 {
		// return ErrEmptyBody
		return nil
	}
	b.index = n
	if pb != nil {
		b.protoBuffer.SetBuf(b.buf[0:n])
		err = b.protoBuffer.Unmarshal(pb)
	}
	return err
}

func (b *Buffer) Unmarshal(pb proto.Message) error {
	if pb == nil || b.Header.Len <= 16 {
		return nil
	}
	b.protoBuffer.SetBuf(b.buf[0 : b.Header.Len-16])
	return b.protoBuffer.Unmarshal(pb)
}

// Body() returns the raw bytes data of last received pb message.
// Only available after ReadMsg()
func (b *Buffer) Body() []byte {
	return b.buf[0:b.index]
}
