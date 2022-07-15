using CSV, DataFrames
include("entsoE_eustdg.jl")

_scs=[["0","1","4"],["0","2","5"],["0","3","6"]]
_yrs=["2019","2020"]
zs=["UK","DE","DK"]#must be in same order as .m file gens

eustdg_datas = entso_scenarios_tss_eustdg(zs,_scs,_yrs)#load the individual time series

eustdg_tss=entso_scenarios_eustdg(eustdg_datas,zs,_scs,_yrs)#combine the time series into scenarios

entso_convex_scenarios_eustdg(eustdg_datas,countries,_scs,_yrs)#convex scenario
