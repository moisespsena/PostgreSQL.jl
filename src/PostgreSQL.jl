#VERSION >= v"0.4" && __precompile__()

module PostgreSQL
    export  Postgres,
            executemany,
            escapeliteral,
            PostgresException,
            PostgresQueryException

    using BinDeps
    @BinDeps.load_dependencies

    include("libpq_interface.jl")
    using .libpq_interface
    using DBI
    using DataFrames
    using DataArrays

    include("types.jl")
    include("dbi_impl.jl")
end
