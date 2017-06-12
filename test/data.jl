import Compat: Libc, @compat

function test_numerics()
    PostgresType = PostgreSQL.PostgresType
    values = @compat Any[Int16(4), Int32(4), Int64(4), Float32(4), Float64(4)]

    types = [PostgresType{:int2}, PostgresType{:int4}, PostgresType{:int8},
        PostgresType{:float4}, PostgresType{:float8}]

    p = convert(Ptr{UInt8}, Libc.malloc(8))
    try
        for i = 1:length(values)
            p = PostgreSQL.storestring!(p, string(values[i]))
            data = PostgreSQL.jldata(types[i], p)
            testsameness(data, values[i])
        end
    finally
        Libc.free(p)
    end

    p = Libc.malloc(8)
    try
        for i = 1:length(values)
            p = PostgreSQL.pgdata(types[i], convert(Ptr{UInt8}, p), values[i])
            data = PostgreSQL.jldata(types[i], p)
            testsameness(data, values[i])
        end
    finally
        Libc.free(p)
    end
end

function test_strings()
    PGType = PostgreSQL.PostgresType
    for typ in [PGType{:varchar}, PGType{:text}, PGType{:bpchar}]
        for str in Any["foobar", "fooba\u211D"]
            p = PostgreSQL.pgdata(typ, convert(Ptr{UInt8}, C_NULL), str)
            try
                data = PostgreSQL.jldata(typ, p)
                @test typeof(str) == typeof(data)
                @test str == data
            finally
                Libc.free(p)
            end
        end
    end
end

function test_arrays()
    PGType = PostgreSQL.PostgresType
    v = [Int32(9), Int32(8)]
    typ = PGType{:_int4}
    p = PostgreSQL.pgdata(typ, convert(Ptr{UInt8}, C_NULL), v)
    try
        data = PostgreSQL.jldata(typ, p)
        @test typeof(v) == typeof(data)
        @test v == data
    finally
        Libc.free(p)
    end

    @test PostgreSQL.jldata(:_varchar, "{1,2,3,4,5}") ==
        Any["1","2","3","4","5"]
    @test PostgreSQL.jldata(:_varchar, "{{1,2},{3,4},{5}}") ==
        Any[Any["1","2"],Any["3","4"],Any["5"]]
    @test PostgreSQL.jldata(:_varchar, "{dfasdf,\"qw,,e{q\\\"we\",'qrer'}") ==
        Any["dfasdf","\"qw","",Any["q\\\"we\"","'qrer'"]]
    @test PostgreSQL.jldata(:_varchar, "{,}") == Any["",""]
    @test PostgreSQL.jldata(:_varchar, "{}") == Any[]
    @test PostgreSQL.jldata(:_varchar, "") == nothing
end

function test_tuples()
    PGType = PostgreSQL.PostgresType
    v = (Int32(9), Int32(8))
    typ = PGType{:_int4}
    p = PostgreSQL.pgdata(typ, convert(Ptr{UInt8}, C_NULL), v)
    try
        data = PostgreSQL.jldata(typ, p)
        @test string(v) == string(tuple(data...))
    finally
        Libc.free(p)
    end
end

function test_bytea()
    typ = PostgreSQL.PostgresType{:bytea}
    bin = (UInt8)[0x01, 0x03, 0x42, 0xab, 0xff]
    p = PostgreSQL.pgdata(typ, convert(Ptr{UInt8}, C_NULL), bin)
    try
        data = PostgreSQL.jldata(typ, p)
        @test typeof(bin) == typeof(data)
        @test bin == data
    finally
        Libc.free(p)
    end
end

function test_json()
    PGType = PostgreSQL.PostgresType
    for typ in Any[PGType{:json}, PGType{:jsonb}]
        dict1 = @compat Dict{String,Any}("bobr dobr" => [1, 2, 3])
        p = PostgreSQL.pgdata(typ, convert(Ptr{UInt8}, C_NULL), dict1)
        try
            dict2 = PostgreSQL.jldata(typ, p)
            @test typeof(dict1) == typeof(dict2)
            @test dict1 == dict2
        finally
            Libc.free(p)
        end
    end
end

test_numerics()
test_strings()
test_arrays()
test_tuples()
test_bytea()
test_json()
