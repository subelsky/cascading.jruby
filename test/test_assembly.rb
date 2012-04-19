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

  # For now, replaced these tests with the trivial observation that you can't
  # follow a Tap with an Every.  Eventually, should support testing within a
  # group_by block.
  def test_create_every
    assert_raise CascadingException do
      assembly = mock_assembly do
        every :aggregator => count_function
      end
      pipe = assembly.tail_pipe
      assert pipe.is_a? Java::CascadingPipe::Every
    end

    assert_raise CascadingException do
      assembly = mock_assembly do
        every :aggregator => count_function('field1', 'field2')
      end
      assert assembly.tail_pipe.is_a? Java::CascadingPipe::Every
    end

    assert_raise CascadingException do
      assembly = mock_assembly do
        every 'field1', :aggregator => count_function
      end
      assert assembly.tail_pipe.is_a? Java::CascadingPipe::Every
      assert_equal ['field1'], assembly.tail_pipe.argument_selector.to_a
    end

    assert_raise CascadingException do
      assembly = mock_assembly do
        every 'line', :aggregator => count_function, :output=>'line_count'
      end
      assert assembly.tail_pipe.is_a? Java::CascadingPipe::Every
      assert_equal ['line'], assembly.tail_pipe.argument_selector.to_a
      assert_equal ['line_count'], assembly.tail_pipe.output_selector.to_a
    end
  end

  def test_create_group_by
    group_by_scope = nil
    assembly = mock_assembly do
      group_by 'line'
      group_by_scope = scope
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    assert_equal ['line'], grouping_fields.to_a

    assert_equal ['offset', 'line'], group_by_scope.values_fields.to_a
    assert_equal ['line'], group_by_scope.grouping_fields.to_a

    assembly = mock_assembly do
      group_by 'offset'
      group_by_scope = scope
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    assert_equal ['offset'], grouping_fields.to_a

    assert_equal ['offset', 'line'], group_by_scope.values_fields.to_a
    assert_equal ['offset'], group_by_scope.grouping_fields.to_a
  end

  def test_create_group_by_many_fields
    group_by_scope = nil
    assembly = mock_assembly do
      group_by 'offset', 'line'
      group_by_scope = scope
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    assert_equal ['offset', 'line'], grouping_fields.to_a

    assert_equal ['offset', 'line'], group_by_scope.values_fields.to_a
    assert_equal ['offset', 'line'], group_by_scope.grouping_fields.to_a
  end

  def test_create_group_by_with_sort
    group_by_scope = nil
    assembly = mock_assembly do
      group_by 'offset', 'line', :sort_by => 'line'
      group_by_scope = scope
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    sorting_fields = assembly.tail_pipe.sorting_selectors['test']

    assert assembly.tail_pipe.is_sorted
    assert !assembly.tail_pipe.is_sort_reversed

    assert_equal ['offset', 'line'], grouping_fields.to_a
    assert_equal ['line'], sorting_fields.to_a

    assert_equal ['offset', 'line'], group_by_scope.values_fields.to_a
    assert_equal ['offset', 'line'], group_by_scope.grouping_fields.to_a
  end

  def test_create_group_by_with_sort_reverse
    group_by_scope = nil
    assembly = mock_assembly do
      group_by 'offset', 'line', :sort_by => 'line', :reverse => true
      group_by_scope = scope
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    sorting_fields = assembly.tail_pipe.sorting_selectors['test']

    assert assembly.tail_pipe.is_sorted
    assert assembly.tail_pipe.is_sort_reversed

    assert_equal ['offset', 'line'], grouping_fields.to_a
    assert_equal ['line'], sorting_fields.to_a

    assert_equal ['offset', 'line'], group_by_scope.values_fields.to_a
    assert_equal ['offset', 'line'], group_by_scope.grouping_fields.to_a
  end

  def test_create_group_by_reverse
    group_by_scope = nil
    assembly = mock_assembly do
      group_by 'offset', 'line', :reverse => true
      group_by_scope = scope
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    sorting_fields = assembly.tail_pipe.sorting_selectors['test']

    assert !assembly.tail_pipe.is_sorted
    assert assembly.tail_pipe.is_sort_reversed

    assert_equal ['offset', 'line'], grouping_fields.to_a
    assert_nil sorting_fields

    assert_equal ['offset', 'line'], group_by_scope.values_fields.to_a
    assert_equal ['offset', 'line'], group_by_scope.grouping_fields.to_a
  end

  def test_create_union
    union_scope = nil
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => 'line'
      union_scope = scope
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy

    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['line'], left_grouping_fields.to_a

    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['line'], right_grouping_fields.to_a

    assert_equal ['offset', 'line'], union_scope.values_fields.to_a
    assert_equal ['line'], union_scope.grouping_fields.to_a

    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => 'offset'
      union_scope = scope
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['offset'], left_grouping_fields.to_a
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['offset'], right_grouping_fields.to_a

    assert_equal ['offset', 'line'], union_scope.values_fields.to_a
    assert_equal ['offset'], union_scope.grouping_fields.to_a

    assembly = mock_branched_assembly do
      union 'test1', 'test2'
      union_scope = scope
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['offset'], left_grouping_fields.to_a
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['offset'], right_grouping_fields.to_a

    assert_equal ['offset', 'line'], union_scope.values_fields.to_a
    assert_equal ['offset'], union_scope.grouping_fields.to_a
  end

  def test_create_union_many_fields
    union_scope = nil
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => ['offset', 'line']
      union_scope = scope
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy

    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['offset', 'line'], left_grouping_fields.to_a

    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['offset', 'line'], right_grouping_fields.to_a

    assert_equal ['offset', 'line'], union_scope.values_fields.to_a
    assert_equal ['offset', 'line'], union_scope.grouping_fields.to_a
  end

  def test_create_union_with_sort
    union_scope = nil
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => ['offset', 'line'], :sort_by => 'line'
      union_scope = scope
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

    assert_equal ['offset', 'line'], union_scope.values_fields.to_a
    assert_equal ['offset', 'line'], union_scope.grouping_fields.to_a
  end

  def test_create_union_with_sort_reverse
    union_scope = nil
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => ['offset', 'line'], :sort_by => 'line', :reverse => true
      union_scope = scope
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

    assert_equal ['offset', 'line'], union_scope.values_fields.to_a
    assert_equal ['offset', 'line'], union_scope.grouping_fields.to_a
  end

  def test_create_union_reverse
    union_scope = nil
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => ['offset', 'line'], :reverse => true
      union_scope = scope
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

    assert_equal ['offset', 'line'], union_scope.values_fields.to_a
    assert_equal ['offset', 'line'], union_scope.grouping_fields.to_a
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
    assert_raise CascadingException do
      assembly = mock_assembly do
        each('offset', :output => 'offset_copy',
             :filter => Java::CascadingOperation::Identity.new(fields('offset_copy')))
        every(:aggregator => count_function)
      end

      pipe = assembly.tail_pipe
      assert pipe.is_a? Java::CascadingPipe::Every
    end
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
          pass
          debug_scope
        end
        sink 'input', tap('output/test_smoke_test_debug_scope')
      end
    end
  end
end

class TC_AssemblyScenarii < Test::Unit::TestCase
  def test_smoke_test_sequence_file_scheme
    cascade 'smoke' do
      flow 'smoke' do
        source 'input', tap('test/data/data1.txt')
        assembly 'input' do
          pass
        end
        compress_output :default, :block
        sink 'input', tap('output/test_smoke_test_sequence_file_scheme', :scheme => sequence_file_scheme)
      end
    end.complete
  end

  def test_splitter
    flow = flow 'splitter' do
      source 'copy', tap('test/data/data1.txt')

      assembly 'copy' do
        split 'line', :pattern => /[.,]*\s+/, :into=>['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
        assert_size_equals 4
        assert_not_null
        debug :print_fields => true
      end

      sink 'copy', tap('output/test_splitter', :sink_mode => :replace)
    end.complete
  end

  def test_smoke_test_multi_source_tap
    cascade 'multi_source_tap' do
      flow 'multi_source_tap' do
        tap1 = tap 'test/data/data1.txt'
        tap2 = tap 'test/data/data2.txt'
        source 'data', multi_source_tap(tap1, tap2)

        assembly 'data' do
          pass
        end

        sink 'data', tap('output/test_smoke_test_multi_source_tap')
      end
    end.complete
  end

  def test_join1
    join_grouping_fields, join_values_fields = nil, nil
    cascade 'splitter' do
      flow 'splitter' do
        source 'data1', tap('test/data/data1.txt')
        source 'data2', tap('test/data/data2.txt')

        assembly1 = assembly 'data1' do
          split 'line', :pattern => /[.,]*\s+/, :into => ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
          assert_size_equals 4
          assert_not_null
          debug :print_fields => true
        end

        assembly2 = assembly 'data2' do
          split 'line', :pattern => /[.,]*\s+/, :into => ['name',  'id', 'town'], :output => ['name',  'id', 'town']
          assert_size_equals 3
          assert_not_null
          debug :print_fields => true
        end

        assembly 'joined' do
          join assembly1.name, assembly2.name, :on => ['name', 'id'], :declared_fields => ['name', 'score1', 'score2', 'id', 'name2', 'id2', 'town']
          join_grouping_fields = scope.grouping_fields.to_a
          join_values_fields = scope.values_fields.to_a

          assert_size_equals 7
          assert_not_null
        end

        sink 'joined', tap('output/test_join1', :sink_mode => :replace)
      end
    end.complete
    assert_equal ['name', 'id'], join_grouping_fields
    assert_equal ['name', 'score1', 'score2', 'id', 'name2', 'id2', 'town'], join_values_fields
  end

  def test_join2
    join_grouping_fields, join_values_fields = nil, nil
    flow = flow 'splitter' do
      source 'data1', tap('test/data/data1.txt')
      source 'data2', tap('test/data/data2.txt')

      assembly 'data1' do
        split 'line', :pattern => /[.,]*\s+/, :into => ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
        debug :print_fields => true
      end

      assembly 'data2' do
        split 'line', :pattern => /[.,]*\s+/, :into => ['name',  'code', 'town'], :output => ['name',  'code', 'town']
        debug :print_fields => true
      end

      assembly 'joined' do
        join :on => {'data1' => ['name', 'id'], 'data2' => ['name', 'code']}, :declared_fields => ['name', 'score1', 'score2', 'id', 'name2', 'code', 'town']
        join_grouping_fields = scope.grouping_fields.to_a
        join_values_fields = scope.values_fields.to_a
      end

      sink 'joined', tap('output/test_join2', :sink_mode => :replace)
     end.complete
     assert_equal ['name', 'id'], join_grouping_fields
     assert_equal ['name', 'score1', 'score2', 'id', 'name2', 'code', 'town'], join_values_fields
   end

  def test_union
    union_grouping_fields, union_values_fields = nil, nil
    cascade 'union' do
      flow 'union' do
        source 'data1', tap('test/data/data1.txt')
        source 'data2', tap('test/data/data2.txt')

        assembly 'data1' do
          split 'line', :pattern => /[.,]*\s+/, :into => ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
          assert_size_equals 4
          assert_not_null

          project 'name', 'id'
          assert_size_equals 2
        end

        assembly 'data2' do
          split 'line', :pattern => /[.,]*\s+/, :into => ['name',  'code', 'town'], :output => ['name',  'code', 'town']
          assert_size_equals 3
          assert_not_null

          rename 'code' => 'id'
          project 'name', 'id'
          assert_size_equals 2
        end

        assembly 'union' do
          union 'data1', 'data2'
          union_grouping_fields = scope.grouping_fields.to_a
          union_values_fields = scope.values_fields.to_a
        end

        sink 'union', tap('output/test_union', :sink_mode => :replace)
     end
    end.complete
    assert_equal ['name'], union_grouping_fields
    assert_equal ['name', 'id'], union_values_fields
  end
end
