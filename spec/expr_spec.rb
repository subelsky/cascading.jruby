require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe Object do
  it 'should allow expr syntax' do
    test_assembly do
      insert 'foo' => 1, 'bar' => expr('offset:int')
      check_scope :values_fields => ['offset', 'line', 'bar', 'foo']
    end
  end

  it 'should compile expressions' do
    e = ExprStub.new('x:int + y:int')
    e.compile
  end

  it 'should throw an exception for parsing failures' do
    e = ExprStub.new('x:int + doesnotparse y:string')
    lambda{ e.compile }.should raise_error ExprParseException
  end

  it 'should throw an exception for compile failures' do
    e = ExprStub.new('new DoesNotExist(x:int).doesnotcompile()')
    lambda{ e.compile }.should raise_error ExprCompileException
  end

  it 'should throw an exception for compile failures' do
    e = ExprStub.new('true ? x:int : y:string')
    lambda{ e.compile }.should raise_error ExprCompileException
  end

  it 'should evaluate expressions' do
    e = ExprStub.new('x:int + y:int')
    result = e.eval(:x => 2, :y => 3)
    result.should == 5

    e = ExprStub.new('x:int + y:string')
    result = e.eval(:x => 2, :y => 'blah')
    result.should == '2blah'

    e = ExprStub.new('x:long + y:int')
    result = e.eval(:x => 2, :y => 3)
    result.should == 5

    e = ExprStub.new('x:double + y:int')
    result = e.eval(:x => 2.0, :y => 3)
    result.should == 5.0

    e = ExprStub.new('x:float + y:int')
    result = e.eval(:x => 2.0, :y => 3)
    result.should == 5.0

    e = ExprStub.new('x:bool && y:bool')
    result = e.eval(:x => true, :y => false)
    result.should == false
  end

  it 'should evaluate expressions despite argument order' do
    e = ExprStub.new('x:int + y:int')
    result = e.eval(:y => 3, :x => 2)
    result.should == 5
  end

  it 'should throw an exception for invalid actual arguments' do
    e = ExprStub.new('x:int + y:int')
    lambda{ e.eval(:x => 2, :y => 'blah') }.should raise_error ExprArgException

    # Janino does not coerce numeric strings to Java Integers
    e = ExprStub.new('x:int + y:int')
    lambda{ e.eval(:x => 2, :y => '3') }.should raise_error ExprArgException

    # eval should not coerce numeric strings to Java Floats
    e = ExprStub.new('x:int + y:float')
    lambda{ e.eval(:x => 2, :y => '3') }.should raise_error ExprArgException

    # eval should not coerce numeric strings to Java Longs
    e = ExprStub.new('x:long + y:int')
    lambda{ e.eval(:x => '2', :y => 3) }.should raise_error ExprArgException

    e = ExprStub.new('x:float + y:int')
    lambda{ e.eval(:x => 'blah', :y => 3) }.should raise_error ExprArgException

    e = ExprStub.new('x:long + y:int')
    lambda{ e.eval(:x => [], :y => 3) }.should raise_error ExprArgException
  end

  it 'should throw an exception for missing actual arguments' do
    e = ExprStub.new('x:int + y:int')
    lambda{ e.eval(:x => 2) }.should raise_error ExprArgException
  end

  it 'should throw an exception for null actual arguments' do
    e = ExprStub.new('x:int + y:int')
    lambda{ e.eval(:x => 2, :y => nil) }.should raise_error ExprArgException
  end

  it 'should use default actual arguments to test evaluation' do
    e = ExprStub.new('x:int + y:int')
    result = e.test_evaluate
    result.should == 0

    e = ExprStub.new('x:int + y:string')
    result = e.test_evaluate
    result.should == '0'

    e = ExprStub.new('x:long + y:int')
    result = e.test_evaluate
    result.should == 0

    e = ExprStub.new('x:double + y:int')
    result = e.test_evaluate
    result.should == 0.0

    e = ExprStub.new('x:float + y:int')
    result = e.test_evaluate
    result.should == 0.0

    e = ExprStub.new('x:bool && y:bool')
    result = e.test_evaluate
    result.should == false
  end

  it 'should catch missing fields in filter expressions' do
    lambda do
      test_assembly do
        filter :expression => 'doesnotexist:int > offset:int'
      end
    end.should raise_error ExprArgException
  end

  it 'should catch missing fields in insert expressions' do
    lambda do
      test_assembly do
        insert 'foo' => 1, 'bar' => expr('doesnotexist:int + offset:int')
      end
    end.should raise_error ExprArgException
  end
end
