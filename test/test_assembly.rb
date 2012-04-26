require 'test/unit'
require 'cascading'

class TC_Assembly < Test::Unit::TestCase
  def mock_assembly(&block)
    assembly = nil
    flow 'test' do
      source 'test', tap('test/data/data1.txt')
      assembly = assembly 'test', &block
      sink 'test', tap('output/test_mock_assembly')
    end
    assembly
  end

  def mock_branched_assembly(&block)
    assembly = nil
    flow 'mock_branched_assembly' do
      source 'data1', tap('test/data/data1.txt')

      assembly 'data1' do
        branch 'test1' do
          pass
        end
        branch 'test2' do
          pass
        end
      end

      assembly = assembly 'test', &block

      sink 'test', tap('output/test_mock_branched_assembly')
    end
    assembly
  end

  def mock_two_input_assembly(&block)
    assembly = nil
    flow 'mock_two_input_assembly' do
      source 'test1', tap('test/data/data1.txt')
      source 'test2', tap('test/data/data2.txt')

      assembly 'test1' do
        split 'line', :pattern => /[.,]*\s+/, :into => ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
      end

      assembly 'test2' do
        split 'line', :pattern => /[.,]*\s+/, :into => ['name',  'id', 'town'], :output => ['name',  'id', 'town']
      end

      assembly = assembly 'test', &block

      sink 'test', tap('output/test_mock_two_input_assembly')
    end
    assembly
  end

  def test_create_assembly_simple
    assembly = nil
    flow 'test_create_assembly_simple' do
      assembly = assembly 'assembly1' do
        # Empty assembly
      end
    end

    assert_not_nil assembly
    assert_equal assembly.name, 'assembly1'
    assert_equal 0, assembly.children.size

    pipe = assembly.tail_pipe
    assert pipe.is_a? Java::CascadingPipe::Pipe
  end

  def test_each_identity
    assembly = mock_assembly do
      each 'offset', :filter => identity
    end

    flow = assembly.parent
    assert_not_nil flow
    assert_not_nil flow.find_child('test')
    assert_equal assembly, flow.find_child('test')
  end

  def test_create_each
    # You can apply an Each to 0 fields
    assembly = mock_assembly do
      each(:filter => identity)
    end
    assert assembly.tail_pipe.is_a? Java::CascadingPipe::Each

    # In which case, it has empty argument and output selectors
    assert_equal 0, assembly.tail_pipe.argument_selector.size
    assert_equal 0, assembly.tail_pipe.output_selector.size

    assembly = mock_assembly do
      each 'offset', :output => 'offset_copy', :filter => Java::CascadingOperation::Identity.new(fields('offset_copy'))
    end
    pipe = assembly.tail_pipe

    assert pipe.is_a? Java::CascadingPipe::Each

    assert_equal ['offset'], pipe.argument_selector.to_a
    assert_equal ['offset_copy'], pipe.output_selector.to_a
  end

  def test_every_cannot_follow_tap
    # Assembly#every is no longer defined; instead, it has moved to
    # Aggregations#every
    assert_raise NoMethodError do
      assembly = mock_assembly do
        every :aggregator => count_function
      end
      pipe = assembly.tail_pipe
      assert pipe.is_a? Java::CascadingPipe::Every
    end
  end

  def test_create_every
      assembly = mock_assembly do
        group_by 'line' do
          every 'line', :aggregator => count_function('count'), :output => 'count'
        end
      end
      assert assembly.tail_pipe.is_a? Java::CascadingPipe::Every
      assert_equal ['line'], assembly.tail_pipe.argument_selector.to_a
      assert_equal ['count'], assembly.tail_pipe.output_selector.to_a

      assembly = mock_assembly do
        group_by 'line' do
          count
        end
      end
      assert assembly.tail_pipe.is_a? Java::CascadingPipe::Every
      assert_equal last_grouping_fields, assembly.tail_pipe.argument_selector
      assert_equal all_fields, assembly.tail_pipe.output_selector
  end

  def test_create_group_by
    assembly = mock_assembly do
      group_by 'line'
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    assert_equal ['line'], grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['line'], assembly.scope.grouping_fields.to_a

    assembly = mock_assembly do
      group_by 'offset'
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    assert_equal ['offset'], grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset'], assembly.scope.grouping_fields.to_a
  end

  def test_create_group_by_many_fields
    assembly = mock_assembly do
      group_by 'offset', 'line'
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    assert_equal ['offset', 'line'], grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_create_group_by_with_sort
    assembly = mock_assembly do
      group_by 'offset', 'line', :sort_by => 'line'
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    sorting_fields = assembly.tail_pipe.sorting_selectors['test']

    assert assembly.tail_pipe.is_sorted
    assert !assembly.tail_pipe.is_sort_reversed

    assert_equal ['offset', 'line'], grouping_fields.to_a
    assert_equal ['line'], sorting_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_create_group_by_with_sort_reverse
    assembly = mock_assembly do
      group_by 'offset', 'line', :sort_by => 'line', :reverse => true
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    sorting_fields = assembly.tail_pipe.sorting_selectors['test']

    assert assembly.tail_pipe.is_sorted
    assert assembly.tail_pipe.is_sort_reversed

    assert_equal ['offset', 'line'], grouping_fields.to_a
    assert_equal ['line'], sorting_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_create_group_by_reverse
    assembly = mock_assembly do
      group_by 'offset', 'line', :reverse => true
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    sorting_fields = assembly.tail_pipe.sorting_selectors['test']

    assert !assembly.tail_pipe.is_sorted
    assert assembly.tail_pipe.is_sort_reversed

    assert_equal ['offset', 'line'], grouping_fields.to_a
    assert_nil sorting_fields

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_group_by_with_block
    assembly = mock_assembly do
      group_by 'line' do
        count
      end
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::Every

    assert_equal ['line', 'count'], assembly.scope.values_fields.to_a
    assert_equal ['line', 'count'], assembly.scope.grouping_fields.to_a
  end

  def test_create_union
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => 'line'
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy

    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['line'], left_grouping_fields.to_a

    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['line'], right_grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['line'], assembly.scope.grouping_fields.to_a

    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => 'offset'
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['offset'], left_grouping_fields.to_a
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['offset'], right_grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset'], assembly.scope.grouping_fields.to_a

    assembly = mock_branched_assembly do
      union 'test1', 'test2'
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['offset'], left_grouping_fields.to_a
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['offset'], right_grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset'], assembly.scope.grouping_fields.to_a
  end

  def test_create_union_many_fields
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => ['offset', 'line']
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy

    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['offset', 'line'], left_grouping_fields.to_a

    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['offset', 'line'], right_grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_create_union_with_sort
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => ['offset', 'line'], :sort_by => 'line'
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    left_sorting_fields = assembly.tail_pipe.sorting_selectors['test1']
    right_sorting_fields = assembly.tail_pipe.sorting_selectors['test2']

    assert assembly.tail_pipe.is_sorted
    assert !assembly.tail_pipe.is_sort_reversed

    assert_equal ['offset', 'line'], left_grouping_fields.to_a
    assert_equal ['offset', 'line'], right_grouping_fields.to_a
    assert_equal ['line'], left_sorting_fields.to_a
    assert_equal ['line'], right_sorting_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_create_union_with_sort_reverse
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => ['offset', 'line'], :sort_by => 'line', :reverse => true
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    left_sorting_fields = assembly.tail_pipe.sorting_selectors['test1']
    right_sorting_fields = assembly.tail_pipe.sorting_selectors['test2']

    assert assembly.tail_pipe.is_sorted
    assert assembly.tail_pipe.is_sort_reversed

    assert_equal ['offset', 'line'], left_grouping_fields.to_a
    assert_equal ['offset', 'line'], right_grouping_fields.to_a
    assert_equal ['line'], left_sorting_fields.to_a
    assert_equal ['line'], right_sorting_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_create_union_reverse
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => ['offset', 'line'], :reverse => true
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    left_sorting_fields = assembly.tail_pipe.sorting_selectors['test1']
    right_sorting_fields = assembly.tail_pipe.sorting_selectors['test2']

    assert assembly.tail_pipe.is_sorted # FIXME: Missing constructor in wip-255
    assert assembly.tail_pipe.is_sort_reversed

    assert_equal ['offset', 'line'], left_grouping_fields.to_a
    assert_equal ['offset', 'line'], right_grouping_fields.to_a
    assert_equal ['offset', 'line'], left_sorting_fields.to_a # FIXME: Missing constructor in wip-255
    assert_equal ['offset', 'line'], right_sorting_fields.to_a # FIXME: Missing constructor in wip-255

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_union_with_block
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => 'line' do
        count
      end
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::Every

    assert_equal ['line', 'count'], assembly.scope.values_fields.to_a
    assert_equal ['line', 'count'], assembly.scope.grouping_fields.to_a
  end

  def test_union_undefined_inputs
    assert_raise RuntimeError, "Could not find assembly 'doesnotexist' in union" do
      flow 'test_union_undefined_inputs' do
        source 'data1', tap('test/data/data1.txt')

        assembly 'data1' do
          pass
        end

        assembly 'union' do
          union 'doesnotexist', 'data1'
        end

        sink 'union', tap('output/test_union_undefined_inputs')
      end
    end
  end

  def test_create_join
    assembly = mock_two_input_assembly do
      join 'test1', 'test2', :on => 'name'
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::CoGroup

    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['name'], left_grouping_fields.to_a

    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['name'], right_grouping_fields.to_a

    assert_equal ['name', 'score1', 'score2', 'id', 'name_', 'id_', 'town'], assembly.scope.values_fields.to_a
    assert_equal ['name'], assembly.scope.grouping_fields.to_a

    assembly = mock_two_input_assembly do
      join 'test1', 'test2', :on => 'id'
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::CoGroup
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['id'], left_grouping_fields.to_a
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['id'], right_grouping_fields.to_a

    assert_equal ['name', 'score1', 'score2', 'id', 'name_', 'id_', 'town'], assembly.scope.values_fields.to_a
    assert_equal ['id'], assembly.scope.grouping_fields.to_a
  end

  def test_create_join_many_fields
    assembly = mock_two_input_assembly do
      join 'test1', 'test2', :on => ['name', 'id']
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::CoGroup
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['name', 'id'], left_grouping_fields.to_a
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['name', 'id'], right_grouping_fields.to_a

    assert_equal ['name', 'score1', 'score2', 'id', 'name_', 'id_', 'town'], assembly.scope.values_fields.to_a
    assert_equal ['name', 'id'], assembly.scope.grouping_fields.to_a
  end

  def test_create_join_with_declared_fields
    assembly = mock_two_input_assembly do
      join 'test1', 'test2', :on => 'name', :declared_fields => ['a', 'b', 'c', 'd', 'e', 'f', 'g']
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::CoGroup

    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['name'], left_grouping_fields.to_a

    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['name'], right_grouping_fields.to_a

    assert_equal ['a', 'b', 'c', 'd', 'e', 'f', 'g'], assembly.scope.values_fields.to_a
    assert_equal ['name'], assembly.scope.grouping_fields.to_a
  end

  def test_join_with_block
    assembly = mock_two_input_assembly do
      join 'test1', 'test2', :on => 'name' do
        count
      end
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::Every

    assert_equal ['name', 'count'], assembly.scope.values_fields.to_a
    assert_equal ['name', 'count'], assembly.scope.grouping_fields.to_a
  end

  def test_join_undefined_inputs
    assert_raise RuntimeError, "Could not find assembly 'doesnotexist' in join" do
      flow 'test_join_undefined_inputs' do
        source 'data1', tap('test/data/data1.txt')

        assembly 'data1' do
          pass
        end

        assembly 'join' do
          join 'doesnotexist', 'data1', :on => 'name'
        end

        sink 'join', tap('output/test_join_undefined_inputs')
      end
    end
  end

  def test_join_without_on
    assert_raise RuntimeError, 'join requires :on parameter' do
      mock_two_input_assembly do
        join 'test1', 'test2'
      end
    end
  end

  def test_join_invalid_on
    assert_raise RuntimeError, "Unsupported data type for :on in join: 'Fixnum'" do
      mock_two_input_assembly do
        join 'test1', 'test2', :on => 1
      end
    end
  end

  def test_join_empty_on
    assert_raise RuntimeError, 'join requres non-empty :on parameter' do
      mock_two_input_assembly do
        join 'test1', 'test2', :on => []
      end
    end

    assert_raise RuntimeError, 'join requres non-empty :on parameter' do
      mock_two_input_assembly do
        join 'test1', 'test2', :on => {}
      end
    end
  end

  def test_branch_unique
    assembly = mock_assembly do
      branch 'branch1' do
      end
    end

    assert_equal 1, assembly.children.size
  end

  def test_branch_empty
    assembly = mock_assembly do
      branch 'branch1' do
      end

      branch 'branch2' do
        branch 'branch3' do
        end
      end
    end

    assert_equal 2, assembly.children.size
    assert_equal 0, assembly.children['branch1'].children.size
    assert_equal 1, assembly.children['branch2'].children.size
  end

  def test_branch_single
    assembly = mock_assembly do
      branch 'branch1' do
        branch 'branch2' do
          each 'line', :function => identity
        end
      end
    end

    assert_equal 1, assembly.children.size
    assert_equal 1, assembly.children['branch1'].children.size
    assert_equal 0, assembly.children['branch1'].children['branch2'].children.size
  end

  def test_full_assembly
    # Assembly#every is no longer defined; instead, it is located at
    # Aggregations#every
    assert_raise NoMethodError do
      assembly = mock_assembly do
        each('offset', :output => 'offset_copy',
             :filter => Java::CascadingOperation::Identity.new(fields('offset_copy')))
        every(:aggregator => count_function)
      end

      pipe = assembly.tail_pipe
      assert pipe.is_a? Java::CascadingPipe::Every
    end
  end

  def test_sub_assembly
    assembly = mock_assembly do
      sub_assembly Java::CascadingPipeAssembly::Discard.new(tail_pipe, fields('offset'))
    end
    assert_equal ['line'], assembly.scope.values_fields.to_a

    assembly = mock_assembly do
      sub_assembly Java::CascadingPipeAssembly::Retain.new(tail_pipe, fields('offset'))
    end
    assert_equal ['offset'], assembly.scope.values_fields.to_a

    assembly = mock_assembly do
      sub_assembly Java::CascadingPipeAssembly::Rename.new(tail_pipe, fields(['offset', 'line']), fields(['byte', 'line']))
    end
    assert_equal ['byte', 'line'], assembly.scope.values_fields.to_a

    assembly = mock_assembly do
      sub_assembly Java::CascadingPipeAssembly::Unique.new(tail_pipe, fields('line'))
    end
    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_empty_where
    assembly = mock_assembly do
      split 'line', ['name', 'score1', 'score2', 'id'], :pattern => /[.,]*\s+/, :output => ['name', 'score1', 'score2', 'id']
      where
    end
    assert assembly.tail_pipe.is_a? Java::CascadingPipe::Each

    # Empty where compiles away
    assert assembly.tail_pipe.operation.is_a? Java::CascadingOperationRegex::RegexSplitter
  end

  def test_where
    assembly = mock_assembly do
      split 'line', ['name', 'score1', 'score2', 'id'], :pattern => /[.,]*\s+/, :output => ['name', 'score1', 'score2', 'id']
      where 'score1:double < score2:double'
    end
    assert assembly.tail_pipe.is_a? Java::CascadingPipe::Each
    assert assembly.tail_pipe.operation.is_a? Java::CascadingOperationExpression::ExpressionFilter
  end

  def test_where_with_expression
    assembly = mock_assembly do
      split 'line', ['name', 'score1', 'score2', 'id'], :pattern => /[.,]*\s+/, :output => ['name', 'score1', 'score2', 'id']
      where :expression => 'score1:double < score2:double'
    end
    assert assembly.tail_pipe.is_a? Java::CascadingPipe::Each
    assert assembly.tail_pipe.operation.is_a? Java::CascadingOperationExpression::ExpressionFilter
  end

  def test_where_with_import
    assembly = mock_assembly do
      split 'line', ['name', 'score1', 'score2', 'id'], :pattern => /[.,]*\s+/, :output => ['name', 'score1', 'score2', 'id']
      names = ['SMITH', 'JONES', 'BROWN']
      where "import java.util.Arrays;\nArrays.asList(new String[] { \"#{names.join('", "')}\" }).contains(name:string)"
    end
    assert assembly.tail_pipe.is_a? Java::CascadingPipe::Each
    assert assembly.tail_pipe.operation.is_a? Java::CascadingOperationExpression::ExpressionFilter
  end

  def test_smoke_test_debug_scope
    cascade 'smoke' do
      flow 'smoke' do
        source 'input', tap('test/data/data1.txt')
        assembly 'input' do
          debug_scope
          group_by 'line' do
            count
            sum 'offset', :type => :long
            debug_scope
          end
          debug_scope
        end
        sink 'input', tap('output/test_smoke_test_debug_scope')
      end
    end
  end
end
