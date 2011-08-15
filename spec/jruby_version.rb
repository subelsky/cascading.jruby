require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe Object do
  case JRUBY_VERSION
  when '1.2.0'
    it 'should handle Fixnum -> Long for ExprStub#eval' do
      e = ExprStub.new('x:long + y:long')
      result = e.eval(:x => 2, :y => 3)
      result.should == 5
    end

    it 'should handle Fixnum -> Long for ExprStub#test_evaluate' do
      e = ExprStub.new('x:long + y:long')
      result = e.test_evaluate
      result.should == 0
    end
  when '1.4.0'
    # This test previously failed for 1.4.0 (it's duplicated in cascading_spec)
    it 'should handle string and integer field names' do
      f = fields(['a', 1, 'b', 2])
      f.to_a.should == ['a', 1, 'b', 2]
    end

    it 'should handle Fixnum -> Integer for ExprStub#eval' do
      e = ExprStub.new('x:int + y:int')
      result = e.eval(:x => 2, :y => 3)
      result.should == 5
    end

    it 'should handle Fixnum -> Integer for ExprStub#test_evaluate' do
      e = ExprStub.new('x:int + y:int')
      result = e.test_evaluate
      result.should == 0
    end
  when '1.5.3'
    it 'should handle Fixnum -> Integer for ExprStub#eval' do
      e = ExprStub.new('x:int + y:int')
      result = e.eval(:x => 2, :y => 3)
      result.should == 5
    end

    it 'should handle Fixnum -> Integer for ExprStub#test_evaluate' do
      e = ExprStub.new('x:int + y:int')
      result = e.test_evaluate
      result.should == 0
    end
  else
    raise "cascading.jruby has not been tested with JRuby version #{JRUBY_VERSION}"
  end
end
