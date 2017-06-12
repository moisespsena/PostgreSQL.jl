# using DBI
using PostgreSQL
using Base.Test

const bytestring = unsafe_string


(HOST, USER, PASSWORD, DB, PORT) = ("127.0.0.1", "moi", "123456", "julia_test", 5433)
DSN = "postgresql://$USER" * (isempty(PASSWORD) ? "" : ":$PASSWORD") * "@$HOST:$PORT/$DB"

include("testutils.jl")

include("connection.jl")
include("dbi_impl.jl")
include("data.jl")
include("postgres.jl")
#include("dataframes_impl.jl")
