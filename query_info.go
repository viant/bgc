package bgc

//Represents query result into
type QueryResultInfo struct {
	CacheHit            bool
	TotalRows           int
	TotalBytesProcessed int
}

//Set sets info values
func (i *QueryResultInfo) Set(info *QueryResultInfo) {
	if i == nil {
		return
	}
	i.CacheHit = info.CacheHit
	i.TotalRows = info.TotalRows
	i.TotalBytesProcessed = info.TotalBytesProcessed
}
