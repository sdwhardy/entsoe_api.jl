using entsoe_api
const _EE = entsoe_api
using HTTP
using Dates
using DataFrames
using Plots
using CSV
using LightXML
using ZipFile
using Indicators
# Write your package code here.
include("../src/api/functions.jl")

_yr="2020"
_scenario="0"
_country="DK"
_start=_yr*"01010000";_end=_yr*"12312300";GBP2EUR=1.15
#Z1="10YNL----------L"#Netherlands
#Z1="10YBE----------2"#Belgium
#Z1="10Y1001A1001A82H"#Germany
Z1="10YDK-1--------W"#western Denmark, 10YDK-2--------M (eastern)
#Z1="10YGB----------A";#GB
# Balancing Price
Z1_balancing_price=_EE.balancing_price(Z1,_start,_end)# Z1
#Z2_balancing_price=_EE.balancing_price(Z2,_start,_end)# Z2
#Z1_balancing_price=DataFrame(time_stamp=Z1_balancing_price[!,:time_stamp],EUR=Z1_balancing_price[!,:GBP].*GBP2EUR)#if UK
#Z2_balancing_price=DataFrame(time_stamp=Z2_balancing_price[!,:time_stamp],EUR=Z2_balancing_price[!,:EUR])


# Balancing Volume
#Z1_balancing_volume=_EE.balancing_volume(Z1,_start,_end)# Z1lgium
#mw2pu=findmax(Z1_balancing_volume[!,:MWh])[1];Z1_balancing_volume[!,:MWh]=Z1_balancing_volume[!,:MWh]./mw2pu#into PU
#Z2_balancing_volume=_EE.balancing_volume(Z2,_start,_end)# Z2
#mw2pu=findmax(Z2_balancing_volume[!,:MWh])[1];Z2_balancing_volume[!,:MWh]=Z2_balancing_volume[!,:MWh]./mw2pu#into PU=#

#Balancing price volume
#Z1_balancing_price=_EE.price_volume_intersection(Z1_balancing_price, Z1_balancing_volume);unique!(Z1_balancing_price,:time_stamp)
#Z2_balancing_price=_EE.price_volume_intersection(Z2_balancing_price, Z2_balancing_volume)

# Day ahead Price
Z1_day_ahead_price=_EE.day_ahead_price(Z1,_start,_end);unique!(Z1_day_ahead_price,:time_stamp)# Z1
#Z1_day_ahead_price=CSV.read("test/data/input/scenarios/time series data/time series data/Based on "*_yr*" prices/"*_country*"_"*_scenario*".csv")# Scenario
#rename!(Z1_day_ahead_price, Dict("MTU (CET)" => "time_stamp", "Day-ahead Price [EUR/MWh]" => "EUR"))
#Z1_day_ahead_price=DataFrame(time_stamp=Z1_day_ahead_price[!,:time_stamp],EUR=Z1_day_ahead_price[!,:GBP].*GBP2EUR)#GBP to EUR
#=dd=[t["time_stamp"][1:2] for t in eachrow(Z1_day_ahead_price)]
mm=[t["time_stamp"][4:5] for t in eachrow(Z1_day_ahead_price)]
yyyy=[t["time_stamp"][7:10] for t in eachrow(Z1_day_ahead_price)]
hhmm=[t["time_stamp"][12:16] for t in eachrow(Z1_day_ahead_price)]
Z1_day_ahead_price["time_stamp"]=DateTime.(yyyy.*"-".*mm.*"-".*dd.*"T".*hhmm)=#
if (haskey(Z1_day_ahead_price,"Column1"))
    Z1_day_ahead_price=select!(Z1_day_ahead_price, Not(:Column1))
end
if (haskey(Z1_day_ahead_price,"Day-ahead Price [GBP/MWh]"))
    Z1_day_ahead_price=select!(Z1_day_ahead_price, Not("Day-ahead Price [GBP/MWh]"))
end
#Z2_day_ahead_price=_EE.day_ahead_price(Z2,_start,_end);unique!(Z2_day_ahead_price,:time_stamp)# Z2
#Z2_day_ahead_price=DataFrame(time_stamp=Z2_day_ahead_price[!,:time_stamp],EUR=Z2_day_ahead_price[!,:GBP].*GBP2EUR)#GBP to EUR
#Z2_day_ahead_price=DataFrame(time_stamp=Z2_day_ahead_price[!,:time_stamp],EUR=Z2_day_ahead_price[!,:EUR])
#region_summary5=innerjoin(Z1_day_ahead_price,Z2_day_ahead_price, makeunique=true,on=:time_stamp)

#Physical flows
#=physical_flows_Z1Z2=_EE.physical_flows(Z1,Z2,_start,_end)# Belgium to Z2
physical_flows_Z2Z1=_EE.physical_flows(Z2,Z1,_start,_end)# Z2 to Belgium
net_physical_flows=DataFrame(time_stamp=physical_flows_Z1Z2[!,:time_stamp],MWh=physical_flows_Z1Z2[!,:MWh].-physical_flows_Z2Z1[!,:MWh])# net flow
mw2pu=findmax(net_physical_flows[!,:MWh])[1];net_physical_flows[!,:MWh]=net_physical_flows[!,:MWh]./mw2pu#into PU=#

#Scheduled commercial exchanges
#"A05" gross
#=CommercialExchange_Z1Z2_gross=_EE.commercial_exchanges(Z1,Z2,_start,_end,"A05")
CommercialExchange_Z2Z1_gross=_EE.commercial_exchanges(Z2,Z1,_start,_end,"A05")
CommercialExchange_net=DataFrame(time_stamp=_EE.CommercialExchange_Z1Z2_gross[!,:time_stamp],MW=CommercialExchange_Z1Z2_gross[!,:MW].-CommercialExchange_Z2Z1_gross[!,:MW])=#


#"A01" day ahead
#=CommercialExchange_Z2Z1_dayahead=_EE.commercial_exchanges(Z2,Z1,_start,_end,"A01")
CommercialExchange_Z1Z2_dayahead=_EE.commercial_exchanges(Z1,Z2,_start,_end,"A01")=#

#combine prices with bidding strategy
#bidding_prices=_EE.bidding_price(CommercialExchange_Z1Z2_dayahead,Z1_balancing_price,Z1_day_ahead_price,CommercialExchange_Z2Z1_dayahead,Z2_balancing_price,Z2_day_ahead_price,CommercialExchange_Z1Z2_gross,CommercialExchange_Z2Z1_gross,CommercialExchange_net)
#bidding_prices=_EE.bidding_price_0835(CommercialExchange_Z1Z2_dayahead,Z1_balancing_price,Z1_day_ahead_price,CommercialExchange_Z2Z1_dayahead,Z2_balancing_price,Z2_day_ahead_price,CommercialExchange_Z1Z2_gross,CommercialExchange_Z2Z1_gross,CommercialExchange_net)

#set net flows to PU
#mw2pu=findmax(CommercialExchange_net[!,:MW])[1];CommercialExchange_net[!,:MW]=CommercialExchange_net[!,:MW]./mw2pu#into PU

# agregated wind generation
wind_gen_Z1=_EE.agrigated_wind_generation(Z1,_start,_end);unique!(wind_gen_Z1,:time_stamp)# Z1
mw2pu=findmax(wind_gen_Z1[!,:MWh])[1];wind_gen_Z1[!,:MWh]=wind_gen_Z1[!,:MWh]./mw2pu#into PU

#wind_gen_Z2=_EE.agrigated_wind_generation(Z2,_start,_end)# Z2
#mw2pu=findmax(wind_gen_Z2[!,:MWh])[1];wind_gen_Z2[!,:MWh]=wind_gen_Z2[!,:MWh]./mw2pu#into PU

#FCR amount/cost
ts_regup,ts_regdown=_EE.activated_reserves(Z1,_start,_end)
ts_regup_price,ts_regdown_price=_EE.activated_reserves_prices(Z1,_start,_end)

reg_rez=_EE.summarize_hourly_activated_reserves(ts_regup,ts_regdown,ts_regup_price,ts_regdown_price);unique!(reg_rez,:time_stamp)
for (i,updwn) in enumerate(reg_rez["MWh_up"].-reg_rez["MWh_dwn"]); if (updwn>=0);
        reg_rez.MWh_up[i]=updwn;reg_rez.EUR_dwn[i]=0;reg_rez.MWh_dwn[i]=0 else;
        reg_rez.MWh_dwn[i]=abs(updwn);reg_rez.EUR_up[i]=0;reg_rez.MWh_up[i]=0;end;end

#Z1Z2_df=_EE.data_intersection(Z1_balancing_price,Z1_day_ahead_price,wind_gen_Z1,Z2_balancing_price,Z2_day_ahead_price,wind_gen_Z2,net_physical_flows)
#Z1Z2_df=_EE.data_intersection2(bidding_prices,wind_gen_Z1,wind_gen_Z2,CommercialExchange_net)
region_summary=innerjoin(wind_gen_Z1,Z1_day_ahead_price, Z1_balancing_price, reg_rez, makeunique=true,on=:time_stamp)
rename!(region_summary, Dict(:MWh => "Wnd_MWh", :EUR => "EUR_da", :EUR_1 => "EUR_id"))
region_summary = region_summary[completecases(region_summary), :]
disallowmissing!(region_summary)
unique!(region_summary,:time_stamp)# Z1
#CSV.write("./test/data/input/scenarios/"*_yr*"/"*_country*"_"*_scenario*".csv",region_summary)
CSV.write("./test/data/input/"*_country*"data.csv",region_summary)



#mean_dif=sum((region_summary.EUR_1).-(region_summary.EUR))/length(region_summary.time_stamp)
monthly_averages=[]

for s in [region_summary1,region_summary2,region_summary3,region_summary4]
    for (i,t) in enumerate(1:720:length(s.time_stamp))
        if ((t+720)<length(s.time_stamp));t2=t+720;else;t2=length(s.time_stamp);end
        push!(monthly_averages,(i,sum((s.EUR_1[t:t2]).-(s.EUR[t:t2]))/length(s.time_stamp[t:t2])))
    end
end

mean_dif=sum((region_summary.EUR_1).-(region_summary.EUR))/length(region_summary.time_stamp)
mean_dif1=sum((region_summary1.EUR_1).-(region_summary1.EUR))/length(region_summary1.time_stamp)
mean_dif2=sum((region_summary2.EUR_1).-(region_summary2.EUR))/length(region_summary2.time_stamp)
mean_dif3=sum((region_summary3.EUR_1).-(region_summary3.EUR))/length(region_summary3.time_stamp)
mean_dif4=sum((region_summary4.EUR_1).-(region_summary4.EUR))/length(region_summary4.time_stamp)
mean_dif4=sum((region_summary4.EUR_1).-(region_summary4.EUR))/length(region_summary4.time_stamp)

mean_dif=sum((set2.EUR_1).-(set2.EUR))/length(set2.time_stamp)
set=vcat(region_summary,region_summary1,region_summary2,region_summary3,region_summary4)
#set=vcat(region_summary)
sma30=sma(set.EUR_1.-set.EUR,n=24*365)
b=[]
for i in sma30; if (isnan(i)); else; push!(b,i);end ;end

tss=TimeArray(set.time_stamp[end-length(b)+1:end],b)


using TimeSeries
using Plots
plotly()
width=750
height=500
p=plot(size = (width, height),xaxis = ("Year", font(14, "Courier")),ylims=(0,15),yaxis = ("Euro/MWh", font(14, "Courier")))
#p=plot(size = (width, height),xlims=(2017,2021),xaxis = ("Year", font(16, "Courier")),yaxis = ("Mean Diff in Euro/MWh (Z2-Z1)", font(16, "Courier")))
plot!(p,tss,color = :red, label="",size = (width, height))
gui()
#2015-09-22T05:00:00 (5617) - 2015-09-22T18:00:00 (5640)
#2015-10-16T05:00:00 (6145) - 2015-10-16T18:00:00 (6216)
set2=vcat(set[1:5616,:],set[5641:6144,:],set[6217:end,:])
