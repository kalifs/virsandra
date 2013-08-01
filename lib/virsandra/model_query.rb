require_relative 'model_queries/select_query'

module Virsandra
  class ModelQuery

    def initialize(model)
      @model = model
    end

    def select(*columns)
      select_query.select(*columns)
      select_query
    end

    def where(params)
      validate_search_params!(params)
      select_query.where(params)
      select_query
    end

    def find_by_key
      return {} unless @model.valid?
      query = Query.select.from(@model.table).where(@model.key)
      query.fetch
    end

    def save
      query = Query.insert.into(@model.table).values(@model.attributes)
      query.fetch
    end

    def delete
      query = Query.delete.from(@model.table).where(@model.key)
      query.fetch
    end

    # def where(params)
    #   query = Query.select.from(@model.table)

    #   unless params.empty?
    #     raise ArgumentError.new("Invalid search terms") unless valid_search_params?(params)
    #     query.where(params)
    #   end

    #   query_enumerator(query)
    # end

    private

    def select_query
      @select_query ||= Virsandra::ModelSelectQuery.new(@model)
    end

    def validate_search_params!(params)
      if params.any? && !valid_search_params?(params)
        raise ArgumentError.new("Invalid search terms #{params.inspect}")
      end
    end

    def valid_search_params?(params)
      params.keys.all? { |key| @model.column_names.include?(key.to_sym) }
    end

    def query_enumerator(query)
      Enumerator.new do |yielder|
        query.execute.each do |row|
          record = @model.new(row.to_hash)
          yielder.yield record
        end
      end
    end

  end
end
