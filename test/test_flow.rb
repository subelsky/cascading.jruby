require 'test/unit'
require 'cascading'

class TC_Flow < Test::Unit::TestCase
  def test_flow
    flow = flow 'flow' do
      $a1 = assembly 'assembly1' do
      end

      $a2 = assembly 'assembly2' do
      end
    end

    assert_equal 2, flow.children.size
    assert_equal $a1, flow.children['assembly1']
    assert_equal $a1, flow.find_child('assembly1')
    assert_equal $a2, flow.last_child
    assert_equal $a2, flow.find_child('assembly2')
    assert_equal ['assembly1', 'assembly2'], flow.child_names

    assert_equal flow, $a1.parent
    assert_equal flow, $a2.parent

    assert_nil flow.parent
    assert_equal flow, flow.root
    assert_equal flow, flow.last_child.root

    assert_equal 'flow', flow.qualified_name
    assert_equal 'flow.assembly2', flow.last_child.qualified_name
  end

  def test_ambiguous_assembly_names
    flow = flow 'flow' do
      source 'a', tap('test/data/data1.txt')

      $a1 = assembly 'a' do
        pass
      end

      $a2 = assembly 'a' do
        pass
      end

      $x = assembly 'x' do
        union 'a'
      end
    end

    # FIXME: 'a' assemblies and qualified names collide
    assert_equal 2, flow.children.size
    assert_equal 'flow.a', $a1.qualified_name
    assert_equal 'flow.a', $a2.qualified_name

    # Ordered child names do not collide
    assert_equal ['a', 'a', 'x'], flow.child_names

    # FIXME: assembly defined last wins
    assert_equal $a2, flow.find_child('a')

    assert_equal 1, $x.tail_pipe.heads.size
    assert_equal $a2.head_pipe, $x.tail_pipe.heads.first
  end

  def test_ambiguous_branch_names
    flow = flow 'flow' do
      source 'a', tap('test/data/data1.txt')
      source 'b', tap('test/data/data1.txt')

      assembly 'a' do
        $b1 = branch 'b' do
          pass
        end
      end

      $b2 = assembly 'b' do
        pass
      end

      $x = assembly 'x' do
        union 'b'
      end
    end

    # FIXME: 'b' assemblies collide even though qualified names don't
    assert_equal 3, flow.children.size
    assert_equal 'flow.a.b', $b1.qualified_name
    assert_equal 'flow.b', $b2.qualified_name
    assert_equal ['a', 'b', 'x'], flow.child_names

    # FIXME: branch hit by depth-first serach first
    assert_equal $b1, flow.find_child('b')

    assert_equal 1, $x.tail_pipe.heads.size
    assert_equal $b1.parent.head_pipe, $x.tail_pipe.heads.first
  end
end
