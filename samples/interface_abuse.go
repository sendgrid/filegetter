type s3ClientMaker interface {
	NewV2(string, string, string bool) objectGetter
}

type s3ObjectGetter interface {
	GetObject(string, string) (stater, error)
}

type s3Stater interface {
	Stat() (interface{}, error)
}