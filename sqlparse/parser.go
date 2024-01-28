package sqlparse

import (
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/pingcap/tidb/parser"

	"github.com/polarsignals/frostdb/query"
)

type Parser struct {
	p *parser.Parser
}

func NewParser() *Parser {
	return &Parser{p: parser.New()}
}

type ParseResult struct {
	Explain bool
	Plan    query.Builder
}

// ExperimentalParse uses the provided query builder to build a FrostDB query
// specified using the provided SQL.
// TODO(asubiotto): This API will change over time. Currently,
// queryEngine.ScanTable is provided as a starting point and no table needs to
// be specified in the SQL statement. Additionally, the idea is to change to
// creating logical plans directly (rather than through a builder).
func (p *Parser) ExperimentalParse(
	builder query.Builder,
	dynColNames []string,
	sql string,
	schema *parquet.Schema,
) (ParseResult, error) {
	asts, _, err := p.p.Parse(sql, "", "")
	if err != nil {
		return ParseResult{}, err
	}

	if len(asts) != 1 {
		return ParseResult{}, fmt.Errorf("cannot handle multiple asts, found %d", len(asts))
	}

	v := newASTVisitor(builder, dynColNames, schema)
	asts[0].Accept(v)
	if v.err != nil {
		return ParseResult{}, v.err
	}

	return ParseResult{Explain: v.explain, Plan: v.builder}, nil
}
