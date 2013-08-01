require 'spec_helper'

describe Virsandra::ModelQuery do
  let(:select_query_class){ Virsandra::ModelSelectQuery }
  let(:model) do
    double("model",
      table: "my_table",
      column_names: [:id, :name, :score]
    )
  end
  let(:query){ described_class.new(model) }

  describe "#select" do
    it "should call select on query class" do
      select_query_class.any_instance.should_receive(:select).with("name")
      query.select("name").should be_kind_of(select_query_class)
    end
  end

  describe "#where" do
    it "should call where on query class" do
      select_query_class.any_instance.should_receive(:where).with({id: 1})
      query.where(id: 1).should be_kind_of(select_query_class)
    end

    context "unknown keys given" do
      it "should raise an error" do
        expect{ query.where(uuid: 1) }.to raise_error(ArgumentError, /Invalid search terms/)
      end
    end
  end

  context "old style" do
    let(:model) {
      stub(:model,
           attributes: {id: 123, name: 'Funky Circle'},
           table: :companies,
           key: {id: 123})
    }

    let(:model_query) { Virsandra::ModelQuery.new(model) }

    it "stores the model on initialization" do
      model_query
      model_query.instance_variable_get(:@model).should == model
    end

    it "saves the model" do
      query = stub(into: '', values: '', fetch: '')

      Virsandra::Query.should_receive(:insert).and_return(query)

      query.should_receive(:into).with(:companies).and_return(query)
      query.should_receive(:values).with(id: 123, name: "Funky Circle").and_return(query)
      query.should_receive(:fetch)
      model_query.save
    end

    it "can retrieve the hash row from cassandra using the model's key" do
      query = stub(from: '', where: '', fetch: '')

      Virsandra::Query.should_receive(:select).and_return(query)
      model.should_receive(:valid?).and_return(true)
      query.should_receive(:from).with(:companies).and_return(query)
      query.should_receive(:where).with(id: 123).and_return(query)
      query.should_receive(:fetch)

      model_query.find_by_key
    end

    it "doesn't fetch a row if the model is invalid" do
      model.should_receive(:valid?).and_return(false)
      model_query.find_by_key.should == {}
    end

    it "deletes the model" do
      query = stub(from: '', where: '', fetch: '')

      Virsandra::Query.should_receive(:delete).and_return(query)
      query.should_receive(:from).with(:companies).and_return(query)
      query.should_receive(:where).with(id: 123).and_return(query)
      query.should_receive(:fetch)

      model_query.delete
    end
  end

end
