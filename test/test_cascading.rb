require 'test/unit'
require 'cascading'

class TC_Cascading < Test::Unit::TestCase
  def test_fields_field
    result = fields(all_fields)
    assert result == all_fields
  end

  def test_fields_single
    declared = "Field1"

    result = fields(declared)

    assert result.size == 1

    assert_equal declared, result.get(0) 
  end

  def test_fields_multiple
    declared = ["Field1", "Field2", "Field3"]

    result = fields(declared)

    assert result.size == 3

    assert_equal declared[0], result.get(0)
    assert_equal declared[1], result.get(1)
    assert_equal declared[2], result.get(2) 
  end

  def test_tap
    tap = tap('/tmp')
    assert tap.is_a? Cascading::Tap
    assert_equal '/tmp', tap.path
    assert_equal text_line_scheme, tap.scheme

    assert tap.local?
    assert_equal '/tmp', tap.hadoop_tap.identifier
    assert tap.hadoop_tap.is_a? Java::CascadingTapHadoop::Hfs

    assert tap.hadoop?
    assert_equal '/tmp', tap.local_tap.identifier
    assert tap.local_tap.is_a? Java::CascadingTapLocal::FileTap
  end
end
