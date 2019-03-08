package address

import ()

//protect the address

const (
	MAX_ADDRESS = (1 << 40) - 1
)

type Address uint64

func AddressFromU64(val uint64) Address {
	if val > MAX_ADDRESS {
		panic("failed to create Address")
	}
	return Address(uint64(val))
}

func (a Address) AsU64() uint64 {
	return uint64(a)
}

func AddressFromU32(val uint32) Address {
	return Address(uint64(val))
}

func (left Address) Add(right Address) Address {
	return AddressFromU64(uint64(left) + uint64(right))
}

func (left Address) Sub(right Address) Address {
	if uint64(right) > uint64(left) {
		panic("Address: sub is wrong")
	}
	return AddressFromU64(uint64(left) - uint64(right))
}
