require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

context ExprStub do
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
    lambda{ e.compile }.should raise_error CascadingException
  end

  it 'should throw an exception for compile failures' do
    e = ExprStub.new('new DoesNotExist(x:int).doesnotcompile()')
    lambda{ e.compile }.should raise_error CascadingException
  end

  it 'should throw an exception for compile failures' do
    e = ExprStub.new('true ? x:int : y:string')
    lambda{ e.compile }.should raise_error CascadingException
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
    lambda{ e.eval(:x => 2, :y => 'blah') }.should raise_error CascadingException

    # Janino does not coerce numeric strings to Java Integers
    e = ExprStub.new('x:int + y:int')
    lambda{ e.eval(:x => 2, :y => '3') }.should raise_error CascadingException

    # eval should not coerce numeric strings to Java Floats
    e = ExprStub.new('x:int + y:float')
    lambda{ e.eval(:x => 2, :y => '3') }.should raise_error CascadingException

    # eval should not coerce numeric strings to Java Longs
    e = ExprStub.new('x:long + y:int')
    lambda{ e.eval(:x => '2', :y => 3) }.should raise_error CascadingException

    # eval should not coerce floats to Java Longs
    e = ExprStub.new('x:long + y:int')
    lambda{ e.eval(:x => 2.0, :y => 3) }.should raise_error CascadingException

    # eval should not coerce integers to Java Floats
    e = ExprStub.new('x:int + y:float')
    lambda{ e.eval(:x => 2, :y => 3) }.should raise_error CascadingException

    e = ExprStub.new('x:float + y:int')
    lambda{ e.eval(:x => 'blah', :y => 3) }.should raise_error CascadingException

    e = ExprStub.new('x:long + y:int')
    lambda{ e.eval(:x => [], :y => 3) }.should raise_error CascadingException

    e = ExprStub.new('x:long + y:int')
    lambda{ e.eval(:x => nil, :y => 3) }.should raise_error CascadingException
  end

  it 'should throw an exception for missing actual arguments' do
    e = ExprStub.new('x:int + y:int')
    lambda{ e.eval(:x => 2) }.should raise_error ExprArgException
  end

  it 'should ignore extraneous actual arguments' do
    e = ExprStub.new('x:int + y:int')
    result = e.eval(:x => 2, :y => 3, :z => 'unused')
    result.should == 5
  end

  it 'should use default actual arguments to validate' do
    e = ExprStub.new('x:int + y:int')
    result = e.validate
    result.should == 0

    e = ExprStub.new('x:long + y:int')
    result = e.validate
    result.should == 0

    e = ExprStub.new('x:double + y:int')
    result = e.validate
    result.should == 0.0

    e = ExprStub.new('x:float + y:int')
    result = e.validate
    result.should == 0.0

    e = ExprStub.new('x:bool && y:bool')
    result = e.validate
    result.should == false

    e = ExprStub.new('x:int + y:string')
    result = e.validate
    result.should == '0null'

    e = ExprStub.new('x:string + y:string')
    result = e.validate
    result.should == 'nullnull'
  end

  it 'should fail to validate these expressions with default actual arguments' do
    e = ExprStub.new('x:string.indexOf("R") == -1')
    lambda { e.validate }.should raise_error CascadingException

    e = ExprStub.new('x:string.substring(0, 8)')
    lambda { e.validate }.should raise_error CascadingException
  end

  it 'should allow overriding default actual arguments for validation' do
    e = ExprStub.new('x:string.indexOf("R") == -1')
    result = e.validate(:x => 'nothinghere')
    result.should == true

    e = ExprStub.new('x:string.substring(0, 8)')
    result = e.validate(:x => 'atleast8chars')
    result.should == 'atleast8'
  end

  it 'should allow overriding default actual arguments for validation via expr' do
    expr('x:string.indexOf("R") == -1', :validate_with => { :x => 'nothinghere' })
    expr('x:string.substring(0, 8)', :validate_with => { :x => 'atleast8chars' })
  end

  it 'should allow overriding default actual arguments for validation via filter' do
    test_assembly do
      filter :expression => 'line:string.indexOf("R") == -1', :validate_with => { :line => 'nothinghere' }
      check_scope :values_fields => ['offset', 'line']
    end
  end

  it 'should allow overriding default actual arguments for validation via where' do
    test_assembly do
      where 'line:string.equals("not_set") && "0".equals(offset:string)', :validate_with => { :line => 'nulls_rejected' }
      check_scope :values_fields => ['offset', 'line']
    end
  end

  it 'should allow disabling validation via expr' do
    expr('x:string.indexOf("R") == -1', :validate => false)
    expr('x:string.substring(0, 8)', :validate => false)
  end

  it 'should allow disabling validation via filter' do
    test_assembly do
      filter :expression => 'line:string.indexOf("R") == -1', :validate => false
      check_scope :values_fields => ['offset', 'line']
    end
  end

  it 'should allow disabling validation via where' do
    test_assembly do
      where 'line:string.indexOf("R") == -1', :validate => false
      check_scope :values_fields => ['offset', 'line']
    end
  end

  it 'should only allow floating point division by zero' do
    e = ExprStub.new('x:float / y:float')
    result = e.validate
    result.nan?.should == true

    e = ExprStub.new('x:double / y:double')
    result = e.validate
    result.nan?.should == true

    # From: http://download.oracle.com/javase/6/docs/api/java/lang/ArithmeticException.html
    # Thrown when an exceptional arithmetic condition has occurred. For
    # example, an integer "divide by zero" throws an instance of this class.

    e = ExprStub.new('x:long / y:long')
    lambda { e.validate }.should raise_error CascadingException

    e = ExprStub.new('x:int / y:int')
    lambda { e.validate }.should raise_error CascadingException
  end

  it 'should catch missing fields in filter expressions' do
    lambda do
      test_assembly do
        filter :expression => 'doesnotexist:int > offset:int'
      check_scope :values_fields => ['offset', 'line', 'bar', 'foo']
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
