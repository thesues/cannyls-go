package util

import ()

func PutUINT64(buf []byte, n uint64) {
	buf[0] = byte(n>>56) & 0xff
	buf[1] = byte(n>>48) & 0xff
	buf[2] = byte(n>>40) & 0xff
	buf[3] = byte(n>>32) & 0xff
	buf[4] = byte(n>>24) & 0xff
	buf[5] = byte(n>>16) & 0xff
	buf[6] = byte(n>>8) & 0xff
	buf[7] = byte(n & 0xff)

}

func GetUINT64(buf []byte) (n uint64) {
	if len(buf) != 8 {
		panic("in GetUint74")
	}
	n = 0
	n |= uint64(buf[0]) << 56
	n |= uint64(buf[1]) << 48
	n |= uint64(buf[2]) << 40
	n |= uint64(buf[3]) << 32
	n |= uint64(buf[4]) << 24
	n |= uint64(buf[5]) << 16
	n |= uint64(buf[6]) << 8
	n |= uint64(buf[7])
	return
}

//binary helper functions
func PutUINT16(buf []byte, n uint16) {
	if len(buf) != 2 {
		panic("in putUint16BigEndian")
	}
	hi := (n & 0xFF00) >> 8
	lo := (n & 0x00FF)
	buf[0] = byte(hi)
	buf[1] = byte(lo)
}

func GetUINT16(buf []byte) (n uint16) {
	if len(buf) != 2 {
		panic("in getUint16BigEndian")
	}
	var hi = uint16(buf[0]) << 8
	var lo = uint16(buf[1])
	return hi | lo
}

func PutUINT40(buf []byte, val uint64) {
	/*
		var val32 = uint32(val >> 8)
		var val8 = uint8(val & 0xFF)
		binary.Write(writer, binary.BigEndian, val32)
		binary.Write(writer, binary.BigEndian, val8)
	*/
	if len(buf) != 5 {
		panic("writeUINT40")
	}
	buf[0] = byte((val >> 32) & 0xFF)
	buf[1] = byte((val >> 24) & 0xFF)
	buf[2] = byte((val >> 16) & 0xFF)
	buf[3] = byte((val >> 8) & 0xFF)
	buf[4] = byte((val) & 0xFF)
}

func GetUINT40(buf []byte) uint64 {
	/*
		var b32 uint32
		var b8 uint8
		err := binary.Read(reader, binary.BigEndian, &b32)
		if err != nil {
			panic("readUINT64")
		}
		err = binary.Read(reader, binary.BigEndian, &b8)
		if err != nil {
			panic("readUINT64")
		}
		return uint64(b32)<<8 | uint64(b8)
	*/
	//var buf [5]byte
	if len(buf) != 5 {
		panic("readUINT64")
	}
	var val uint64
	val |= uint64(buf[0]) << 32
	val |= uint64(buf[1]) << 24
	val |= uint64(buf[2]) << 16
	val |= uint64(buf[3]) << 8
	val |= uint64(buf[4])
	return val

}

func Min(x uint64, y uint64) uint64 {
	if x < y {
		return x
	} else {
		return y
	}
}
