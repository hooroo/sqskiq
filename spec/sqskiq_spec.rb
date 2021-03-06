require 'spec_helper'

describe Sqskiq do

  describe 'number of processors is lesser than 2' do
    let(:options) { [ { processors: 1 }, ].sample }
    
    it 'uses the defaut value of 20' do
      config = Sqskiq.valid_config_from(options)
      config[:num_workers].should eq(20)
      config[:num_fetchers].should eq(2)
      config[:num_batches].should eq(2)
      config[:num_deleters].should eq(2)
    end
  end
  
  describe 'number of processor is greater than 2' do
    
    describe 'with nothing remaining after performing division by 10' do
      let(:options) { { processors: [ 20, 30, 40 ].sample } }
      
      it 'uses the the given value' do
        config = Sqskiq.valid_config_from(options)
        config[:num_workers].should eq(options[:processors])
        config[:num_fetchers].should eq(options[:processors] / 10)
        config[:num_batches].should eq(options[:processors] / 10)
        config[:num_deleters].should eq(options[:processors] / 10)
      end
      
    end
    
    describe 'with remaining value after performing division by 10' do
      let(:options) { { processors: [ 21, 31, 41 ].sample } }
      
      it 'uses the the given value for the processors and apply (processors / 10) + 1 for other pool sizes' do
        config = Sqskiq.valid_config_from(options)
        config[:num_workers].should eq(options[:processors])
        config[:num_fetchers].should eq((options[:processors] / 10) + 1)
        config[:num_batches].should eq((options[:processors] / 10) + 1)
        config[:num_deleters].should eq((options[:processors] / 10) + 1)
      end
      
    end
    
    describe 'and lesser or equals to 10' do
      let(:options) { { processors: Random.rand(2..10) } }
      
      it 'uses the the given value for the processors and apply (processors / 10) for other pool sizes' do
        config = Sqskiq.valid_config_from(options)
        config[:num_workers].should eq(options[:processors])
        config[:num_fetchers].should eq(2)
        config[:num_batches].should eq(2)
        config[:num_deleters].should eq(2)
      end
      
    end
  end
  

end