package file

// MappedBlobCap is the number of sealed GLCB mappings the manager keeps open
// before evicting unpinned ones. Exposed so tests in other packages can prove
// a scenario genuinely exceeds it.
func MappedBlobCap() int { return glcbMappedCap }
