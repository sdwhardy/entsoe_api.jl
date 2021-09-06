#isdefined(Base, :__precompile__) && __precompile__()
module entsoe_api
    using HTTP
    using Dates
    using DataFrames
    using Plots
    using CSV
    using LightXML
    using ZipFile
    # Write your package code here.
    include("api/functions.jl")
end
