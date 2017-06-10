function test_connection()
  println("Using libpq")
    libpq = PostgreSQL.libpq_interface

    println("Checking basic connect")

    conn = connect(Postgres, HOST, USER, PASSWORD, DB, PORT)
    @test isa(conn, PostgreSQL.PostgresDatabaseHandle)
    @test conn.status == PostgreSQL.CONNECTION_OK
    @test errcode(conn) == PostgreSQL.CONNECTION_OK
    @test !conn.closed
    @test bytestring(libpq.PQdb(conn.ptr)) == DB
    @test bytestring(libpq.PQuser(conn.ptr)) == USER
    @test bytestring(libpq.PQport(conn.ptr)) == "$PORT"

    disconnect(conn)
    @test conn.closed
    println("Basic connection passed")

    println("Checking doblock")
    conn = connect(Postgres, HOST, USER, PASSWORD, DB, PORT) do conn
        @test isa(conn, PostgreSQL.PostgresDatabaseHandle)
        @test conn.status == PostgreSQL.CONNECTION_OK
        @test errcode(conn) == PostgreSQL.CONNECTION_OK
        @test !conn.closed
        return conn
    end
    @test conn.closed
    println("Doblock passed")

    println("Testing connection with DSN string")
    conn = connect(Postgres, DSN)
    @test isa(conn, PostgreSQL.PostgresDatabaseHandle)
    @test conn.status == PostgreSQL.CONNECTION_OK
    @test errcode(conn) == PostgreSQL.CONNECTION_OK
    @test !conn.closed
    @test bytestring(libpq.PQdb(conn.ptr)) == DB
    @test bytestring(libpq.PQuser(conn.ptr)) == USER
    @test bytestring(libpq.PQport(conn.ptr)) == "$PORT"

    disconnect(conn)
    @test conn.closed
    println("DSN connection passed")

#=    println("Testing connection with DSN string and doblock")
    conn = connect(Postgres, DSN) do conn
        @test isa(conn, PostgreSQL.PostgresDatabaseHandle)
        @test conn.status == PostgreSQL.CONNECTION_OK
        @test errcode(conn) == PostgreSQL.CONNECTION_OK
        @test !conn.closed
        return conn
    end
    @test conn.closed
    println("DSN connection passed in a do block")=#
end

test_connection()
