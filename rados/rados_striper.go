package rados

// #cgo LDFLAGS: -lrados -lradosstriper
// #include <errno.h>
// #include <stdlib.h>
// #include <rados/librados.h>
// #include <radosstriper/libradosstriper.h>
import "C"

import (
	"time"
	"unsafe"
)

type RadosStriper struct {
	striper C.rados_striper_t
}

func NewRadosStriper(ioctx IOContext) *RadosStriper {
	striper := &RadosStriper{}
	ret := C.rados_striper_create(ioctx.ioctx, &striper.striper)
	if ret == 0 {
		return striper
	}
	return nil
}

func (rs *RadosStriper) Write(oid string, data []byte, offset uint64) error {
	coid := C.CString(oid)
	defer C.free(unsafe.Pointer(coid))

	dataPointer := unsafe.Pointer(nil)
	if len(data) > 0 {
		dataPointer = unsafe.Pointer(&data[0])
	}

	ret := C.rados_striper_write(rs.striper, coid,
		(*C.char)(dataPointer),
		(C.size_t)(len(data)),
		(C.uint64_t)(offset))

	return getError(ret)
}

func (rs *RadosStriper) Read(oid string, data []byte, offset uint64) (int, error) {
	coid := C.CString(oid)
	defer C.free(unsafe.Pointer(coid))

	var buf *C.char
	if len(data) > 0 {
		buf = (*C.char)(unsafe.Pointer(&data[0]))
	}

	ret := C.rados_striper_read(
		rs.striper,
		coid,
		buf,
		(C.size_t)(len(data)),
		(C.uint64_t)(offset))

	if ret >= 0 {
		return int(ret), nil
	}
	return 0, getError(ret)
}

func (rs *RadosStriper) Stat(object string) (stat ObjectStat, err error) {
	var cPsize C.uint64_t
	var cPmtime C.time_t
	cObject := C.CString(object)
	defer C.free(unsafe.Pointer(cObject))

	ret := C.rados_striper_stat(
		rs.striper,
		cObject,
		&cPsize,
		&cPmtime)

	if ret < 0 {
		return ObjectStat{}, getError(ret)
	}
	return ObjectStat{
		Size:    uint64(cPsize),
		ModTime: time.Unix(int64(cPmtime), 0),
	}, nil
}
