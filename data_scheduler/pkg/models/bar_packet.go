package models

import (
	"encoding/binary"
	"io"
	"unsafe"
)

type BarPacket struct {
	Timestamp int64
	Open      float64
	Close     float64
	High      float64
	Low       float64
	Volume    int64
}

func NewBarPacket(timestamp int64, open, close, high, low float64, volume int64) *BarPacket {
	return &BarPacket{
		Timestamp: timestamp,
		Open:      open,
		Close:     close,
		High:      high,
		Low:       low,
		Volume:    volume,
	}
}

func (packet *BarPacket) WriteTo(writer io.Writer) (int64, error) {
	err := binary.Write(writer, binary.LittleEndian, packet)
	if err != nil {
		return 0, err
	}

	return int64(unsafe.Sizeof(*packet)), nil
}

func ReadBarPacket(reader io.Reader) (*BarPacket, error) {
	packet := &BarPacket{}
	err := binary.Read(reader, binary.LittleEndian, packet)
	return packet, err
}
