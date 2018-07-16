package matemap

type MigrationTableName struct {
	SourceSchema string
	SourceTable  string
	TargetSchema string
	TargetTable  string
}

/*

 */
func NewMigrationTableName(_sourceSchema string, _sourceTable string,
	_targetSchema string, _targetTable string) *MigrationTableName {

	return &MigrationTableName{
		SourceSchema: _sourceSchema,
		SourceTable:  _sourceTable,
		TargetSchema: _targetSchema,
		TargetTable:  _sourceTable,
	}
}
