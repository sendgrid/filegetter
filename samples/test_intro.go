// each test will set the following
useRemote := true
expectedRemoteErr := fmt.Error("some remote error")
expectedLocalErr := fmt.Error("some local err")
expectedData := []byte("file data")

// create a new Getter with test params
getter := New(useRemote, "accesskey", "accesssecret")

// override the remote and local getters to use fakes that return some error
getter.remoteFetcher = &fakeRemote{data: expectedData, err: expectedRemoteErr}
getter.localFetcher = &fakeLocal{data: expectedData, err: expectedLocalErr}
fh, source, err := getter.GetMessage("localpath", "host", "bucket", "key")

// assert against the return values that include the file data, the source of the file data (or error), and the error