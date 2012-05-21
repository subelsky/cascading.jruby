require 'test/unit'
require 'cascading'

class TC_Flow < Test::Unit::TestCase
  def test_flow
    a1, a2 = nil, nil
    flow = flow 'flow' do
      a1 = assembly 'assembly1' do
      end

      a2 = assembly 'assembly2' do
      end
    end

    assert_equal 2, flow.children.size
    assert_equal a1, flow.children['assembly1']
    assert_equal a1, flow.find_child('assembly1')
    assert_equal a2, flow.last_child
    assert_equal a2, flow.find_child('assembly2')
    assert_equal ['assembly1', 'assembly2'], flow.child_names

    assert_equal flow, a1.parent
    assert_equal flow, a2.parent

    assert_nil flow.parent
    assert_equal flow, flow.root
    assert_equal flow, flow.last_child.root

    assert_equal 'flow', flow.qualified_name
    assert_equal 'flow.assembly2', flow.last_child.qualified_name
  end

  def test_ambiguous_assembly_names
    ex = assert_raise AmbiguousNodeNameException do
      a1, a2, x = nil, nil, nil
      flow = flow 'flow' do
        source 'a', tap('test/data/data1.txt')

        a1 = assembly 'a' do
          pass
        end

        a2 = assembly 'a' do
          pass
        end

        x = assembly 'x' do
          union 'a'
        end
      end
    end
    assert_equal "Attempted to add 'flow.a', but node named 'a' already exists", ex.message
  end

  def test_ambiguous_branch_names
    # You _can_ define ambiguously named branches
    b1, b2 = nil, nil
    flow = flow 'flow' do
      source 'a', tap('test/data/data1.txt')
      source 'b', tap('test/data/data1.txt')

      assembly 'a' do
        b1 = branch 'b' do
          pass
        end
      end

      b2 = assembly 'b' do
        pass
      end

      sink 'b', tap('output/test_ambiguous_branch_names')
    end

    assert_equal 2, flow.children.size
    assert_equal 'flow.a.b', b1.qualified_name
    assert_equal 'flow.b', b2.qualified_name
    assert_equal ['a', 'b'], flow.child_names

    # You _cannot_ look them up using find_child
    ex = assert_raise AmbiguousNodeNameException do
      flow.find_child('b')
    end
    assert_equal "Ambiguous lookup of child by name 'b'; found 'flow.b', 'flow.a.b'", ex.message

    # Which means you cannot sink them
    ex = assert_raise AmbiguousNodeNameException do
      flow.complete # NOTE: We must complete for sink to raise
    end
    assert_equal "Ambiguous lookup of child by name 'b'; found 'flow.b', 'flow.a.b'", ex.message

    # And you cannot use them for join or union
    ex = assert_raise AmbiguousNodeNameException do
      b1, b2, x = nil, nil, nil
      flow = flow 'flow' do
        source 'a', tap('test/data/data1.txt')
        source 'b', tap('test/data/data1.txt')

        assembly 'a' do
          b1 = branch 'b' do
            pass
          end
        end

        b2 = assembly 'b' do
          pass
        end

        x = assembly 'x' do
          union 'b'
        end
      end
    end
    assert_equal "Ambiguous lookup of child by name 'b'; found 'flow.b', 'flow.a.b'", ex.message
  end
end
