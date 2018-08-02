package mysqlapplybinlog


// 定义是减少还是增加 应用 binlog 的行数
const (
	AODNAB_TYPE_ADD      = iota
	AODNAB_TYPE_DELETE
)

// 用于操作是添加还是减少还需要应用的binlog行数
type AddOrDeleteNeedApplyBinlog struct {
	Key string
	Type int
	Num int
}

/* 新建一个 添加还是减少需要应用binlog的行数

 */
func NewAddOrDeleteNeedApplyBinlog(_key string, _type int, _num int) *AddOrDeleteNeedApplyBinlog {
	return &AddOrDeleteNeedApplyBinlog{
		Key: _key,
		Type: _type,
		Num: _num,
	}
}
