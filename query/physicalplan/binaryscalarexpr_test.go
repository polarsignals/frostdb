package physicalplan

import (
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/arrow/scalar"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

func BenchmarkBinaryScalarOperation(b *testing.B) {
	ab := array.NewInt64Builder(memory.DefaultAllocator)
	for i := int64(0); i < 1_000_000; i++ {
		ab.Append(i % 10)
	}

	arr := ab.NewInt64Array()
	ab.Release()

	s := scalar.NewInt64Scalar(4) // chosen by fair dice roll. guaranteed to be random.

	operators := []logicalplan.Op{
		logicalplan.OpAnd,
		logicalplan.OpEq,
		logicalplan.OpNotEq,
		logicalplan.OpLt,
		logicalplan.OpLtEq,
		logicalplan.OpGt,
		logicalplan.OpGtEq,
	}

	for _, op := range operators {
		b.Run(op.String(), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = BinaryScalarOperation(arr, s, op)
			}
		})
	}
}

func BenchmarkBinaryScalarContains(b *testing.B) {
	lines := []string{
		"Hot chicken dolor truffaut knausgaard, ramps shaman skateboard neutral milk hotel af letterpress kickstarter art party church-key hoodie. Butcher typewriter cold-pressed, williamsburg voluptate organic bushwick roof party banjo live-edge bodega boys. Fixie praxis incididunt health goth, in salvia cray commodo fam trust fund. Kale chips street art slow-carb shabby chic occaecat echo park messenger bag fam meggings DSA offal leggings. Taiyaki yes plz everyday carry sartorial eiusmod fugiat williamsburg elit jean shorts pop-up grailed. Authentic exercitation palo santo praxis distillery. Fam 3 wolf moon tbh pabst la croix lumbersexual sunt subway tile normcore.",
		"Vexillologist art party cloud bread ipsum. Mlkshk intelligentsia taiyaki semiotics ut. Before they sold out YOLO health goth poke plaid. Literally edison bulb excepteur ethical man bun tacos, viral retro ipsum bicycle rights four dollar toast. Franzen roof party enim green juice iPhone officia. Gorpcore squid vegan dolore mumblecore quinoa lomo ullamco. Pork belly labore intelligentsia skateboard.",
		"Actually lumbersexual praxis art party pug church-key yuccie JOMO tilde mollit reprehenderit. Fanny pack chambray literally fam biodiesel, single-origin coffee marxism everyday carry velit la croix direct trade JOMO. Offal praxis live-edge plaid cloud bread cupidatat est wayfarers farm-to-table +1 sed echo park magna. Hella raw denim cornhole slow-carb marxism gorpcore. Pop-up voluptate cornhole mumblecore tbh hexagon heirloom echo park activated charcoal eu. Beard kickstarter ethical pabst fingerstache bespoke tousled kogi 8-bit lyft try-hard bodega boys ullamco readymade.",
		"Cardigan bruh cronut, farm-to-table marfa ex aliqua subway tile kinfolk. Brunch tbh adaptogen fam chartreuse, irure palo santo shaman JOMO neutra. Paleo thundercats non hashtag ennui. In readymade four loko non ea. Kinfolk subway tile kogi, waistcoat copper mug lomo pug duis sunt.",
		"Deserunt you probably haven't heard of them lyft migas. Palo santo reprehenderit vegan, marfa proident gochujang kale chips paleo edison bulb ascot kinfolk banjo. Copper mug seitan quis officia flannel everyday carry sed cold-pressed. Blue bottle 3 wolf moon air plant, ex salvia fixie crucifix you probably haven't heard of them keffiyeh enim meditation kinfolk four dollar toast poke. 8-bit banh mi godard, offal cred fanny pack readymade taiyaki. Fixie glossier waistcoat, incididunt non pinterest yes plz ullamco pour-over aesthetic lo-fi.",
	}

	ab := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	as := array.NewStringBuilder(memory.DefaultAllocator)
	ad := array.NewDictionaryBuilder(memory.DefaultAllocator, &arrow.DictionaryType{
		IndexType: &arrow.Uint32Type{},
		ValueType: arrow.BinaryTypes.String,
	}).(*array.BinaryDictionaryBuilder)

	for i := 0; i < 10_000; i++ {
		line := lines[i%len(lines)]
		ab.AppendString(line)
		as.Append(line)
		require.NoError(b, ad.AppendString(line))
	}

	arrayBinary := ab.NewBinaryArray()
	arrayString := as.NewStringArray()
	arrayDict := ad.NewDictionaryArray()
	ab.Release()
	as.Release()
	ad.Release()

	s := scalar.NewStringScalar("vegan")

	types := []struct {
		name string
		arr  arrow.Array
	}{{
		name: "binary",
		arr:  arrayBinary,
	}, {
		name: "string",
		arr:  arrayString,
	}, {
		name: "dictionary",
		arr:  arrayDict,
	}}

	b.ResetTimer()
	b.ReportAllocs()

	for _, tt := range types {
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = BinaryScalarOperation(tt.arr, s, logicalplan.OpContains)
			}
		})
	}
}

func TestBinaryScalarOperationNotImplemented(t *testing.T) {
	ab := array.NewInt64Builder(memory.DefaultAllocator)
	arr := ab.NewInt64Array()
	ab.Release()

	s := scalar.NewInt64Scalar(4) // chosen by fair dice roll. guaranteed to be random.

	_, err := BinaryScalarOperation(arr, s, logicalplan.OpAnd)
	require.Equal(t, err, ErrUnsupportedBinaryOperation)
}

func Test_ArrayScalarCompute_Leak(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer allocator.AssertSize(t, 0)

	ab := array.NewInt64Builder(allocator)
	defer ab.Release()

	ab.AppendValues([]int64{1, 2, 3}, nil)
	arr := ab.NewInt64Array()
	defer arr.Release()

	s := scalar.NewInt64Scalar(4)
	_, err := ArrayScalarCompute("equal", arr, s)
	require.NoError(t, err)
}
