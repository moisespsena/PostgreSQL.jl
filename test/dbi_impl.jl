#import DataFrames: DataFrameRow
#import DataArrays: NA
import Compat: @compat, parse


function test_dbi()
    #@test Postgres <: DBI.DatabaseSystem

    conn = connect(Postgres, HOST, USER, PASSWORD, DB, PORT)

    #@test isa(conn, DBI.DatabaseHandle)
    @test isdefined(conn, :status)

    stmt = prepare(conn, "SELECT 1::bigint, 2.0::double precision, 'foo'::character varying, " *
                         "'foo'::character(10), NULL;")
    result = execute(stmt)
    testdberror(result, PostgreSQL.PGRES_TUPLES_OK)

    iterresults = Vector{Any}[]
    for row in result
        @test row[1] === @compat Int64(1)
        @test_approx_eq row[2] 2.0
        @test typeof(row[2]) == Float64
        @test row[3] == "foo"
        @test typeof(row[3]) <: AbstractString
        @test row[4] == "foo       "
        @test typeof(row[4]) <: AbstractString
        @test row[5] === nothing
        push!(iterresults, row)
    end

    allresults = fetchall(result)

    @test iterresults == allresults

    #dfresults = fetchdf(result)

    #dfrow = Any[x[2] for x in DataFrameRow(dfresults, 1)]
    #dfrow[5] = nothing

    #@test dfrow == allresults[1]

    finish(stmt)


    create_str = """CREATE TEMPORARY TABLE testdbi (
            id serial PRIMARY KEY,
            combo double precision,
            quant double precision,
            name varchar,
            color text,
            bin bytea,
            is_planet bool,
            num_int numeric(80,0),
            num_float numeric(80,10)
        );"""

    run(conn, create_str)

    data = Vector[
        Any[1, 4, "Spam spam eggs and spam", "red", (UInt8)[0x01, 0x02, 0x03, 0x04], nothing, BigInt(123), parse(BigFloat, "123.4567")],
        Any[5, 8, "Michael Spam Palin", "blue", (UInt8)[], true, -3, parse(BigFloat, "-3.141592653")],
        Any[3, 16, nothing, nothing, nothing, false, nothing, nothing],
        Any[nothing, 32, "Foo", "green", (UInt8)[0xfe, 0xdc, 0xba, 0x98, 0x76], true, 9876, parse(BigFloat, "9876.54321")]
        #Any[NA, 32, "Foo", "green", (UInt8)[0xfe, 0xdc, 0xba, 0x98, 0x76], true, 9876, parse(BigFloat, "9876.54321")]
    ]

    insert_str = "INSERT INTO testdbi (combo, quant, name, color, bin, is_planet, num_int, num_float) " *
                 "VALUES(\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8);"

    stmt = prepare(conn, insert_str)
    for row in data
        execute(stmt, row)
        testdberror(stmt, PostgreSQL.PGRES_COMMAND_OK)
    end
    finish(stmt)

    stmt = prepare(conn, "SELECT combo, quant, name, color, bin, is_planet, num_int, num_float FROM testdbi ORDER BY id;")
    result = execute(stmt)
    testdberror(stmt, PostgreSQL.PGRES_TUPLES_OK)
    rows = fetchall(result)
    @test rows[1] == data[1]
    @test rows[2] == data[2]
    @test rows[3] == data[3]
    @test rows[4][1] == nothing
    @test rows[4][2] == data[4][2]
    @test rows[4][3] == data[4][3]
    @test rows[4][4] == data[4][4]
    @test rows[4][5] == data[4][5]
    @test rows[4][6] == data[4][6]
    @test rows[4][7] == data[4][7]
    @test rows[4][8] == data[4][8]

    finish(stmt)

    disconnect(conn)

    conn = connect(Postgres, HOST, USER, PASSWORD, DB, PORT)
    run(conn, create_str)
    stmt = prepare(conn, insert_str)
    executemany(stmt, data)
    testdberror(stmt, PostgreSQL.PGRES_COMMAND_OK)
    finish(stmt)

    stmt = prepare(conn, "SELECT combo, quant, name, color, bin, is_planet, num_int, num_float FROM testdbi ORDER BY id;")
    result = execute(stmt)
    testdberror(stmt, PostgreSQL.PGRES_TUPLES_OK)
    rows = fetchall(result)
    @test rows[1] == data[1]
    @test rows[2] == data[2]
    @test rows[3] == data[3]
    @test rows[4][1] == nothing
    @test rows[4][2] == data[4][2]
    @test rows[4][3] == data[4][3]
    @test rows[4][4] == data[4][4]
    @test rows[4][5] == data[4][5]
    @test rows[4][6] == data[4][6]
    @test rows[4][7] == data[4][7]
    @test rows[4][8] == data[4][8]

    finish(stmt)

    @test escapeliteral(conn, 3) == 3
    @test escapeliteral(conn, 3.3) == 3.3
    @test escapeliteral(conn, "foo") == "'foo'"
    @test escapeliteral(conn, "fo\u2202") == "'fo\u2202'"
    @test escapeliteral(conn, SubString("myfood", 3, 5)) == "'foo'"

    disconnect(conn)
end

test_dbi()
