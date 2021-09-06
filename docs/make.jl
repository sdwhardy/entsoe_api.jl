using entsoe_api
using Documenter

DocMeta.setdocmeta!(entsoe_api, :DocTestSetup, :(using entsoe_api); recursive=true)

makedocs(;
    modules=[entsoe_api],
    authors="Su-",
    repo="https://github.com/sdwhardy/entsoe_api.jl/blob/{commit}{path}#{line}",
    sitename="entsoe_api.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://sdwhardy.github.io/entsoe_api.jl",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/sdwhardy/entsoe_api.jl",
)
