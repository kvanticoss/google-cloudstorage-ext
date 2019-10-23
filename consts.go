package gcsext

const (
	bufferSize = 1024 * 1024 * 5 // 5 MBytes
)

var (
	// placeholderMD5 is the MD5 hash for a virtual blob-objected used to denote folders in GCS, the blobs only contains the text "placeholder"
	placeholderMD5 = []byte{0x6a, 0x99, 0xc5, 0x75, 0xab, 0x87, 0xf8, 0xc7, 0xd1, 0xed, 0x1e, 0x52, 0xe7, 0xe3, 0x49, 0xce}
)