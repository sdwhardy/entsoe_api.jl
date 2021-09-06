using entsoe_api
const _EE = entsoe_api


_start="202001010000";_end="202012312300";GBP2EUR=1.17
#Z1="10YBE----------2"#Belgium
Z1="10Y1001A1001A82H"#Germany
Z1="10YDK-1--------W"#western Denmark, 10YDK-2--------M (eastern)
UK="10YGB----------A";#GB
# Balancing Price
BE_balancing_price=_EE.balancing_price(Z1,_start,_end)# Belgium
UK_balancing_price=_EE.balancing_price(UK,_start,_end)# UK
UK_balancing_price=DataFrame(time_stamp=UK_balancing_price[!,:time_stamp],EUR=UK_balancing_price[!,:GBP].*GBP2EUR)=#

# Balancing Volume
BE_balancing_volume=_EE.balancing_volume(Z1,_start,_end)# Z1lgium
mw2pu=findmax(BE_balancing_volume[!,:MWh])[1];BE_balancing_volume[!,:MWh]=BE_balancing_volume[!,:MWh]./mw2pu#into PU
UK_balancing_volume=_EE.balancing_volume(UK,_start,_end)# UK
mw2pu=findmax(UK_balancing_volume[!,:MWh])[1];UK_balancing_volume[!,:MWh]=UK_balancing_volume[!,:MWh]./mw2pu#into PU=#

#Balancing price volume
BE_balancing_price=_EE.price_volume_intersection(BE_balancing_price, BE_balancing_volume);unique!(UK_balancing_price,:time_stamp)
UK_balancing_price=_EE.price_volume_intersection(UK_balancing_price, UK_balancing_volume)

# Day ahead Price
BE_day_ahead_price=_EE.day_ahead_price(Z1,_start,_end);unique!(UK_day_ahead_price,:time_stamp)# Belgium
UK_day_ahead_price=_EE.day_ahead_price(UK,_start,_end)# UK
UK_day_ahead_price=DataFrame(time_stamp=UK_day_ahead_price[!,:time_stamp],EUR=UK_day_ahead_price[!,:GBP].*GBP2EUR)

#Physical flows
#=physical_flows_beuk=_EE.physical_flows(BE,UK,_start,_end)# Belgium to UK
physical_flows_ukbe=_EE.physical_flows(UK,BE,_start,_end)# UK to Belgium
net_physical_flows=DataFrame(time_stamp=physical_flows_beuk[!,:time_stamp],MWh=physical_flows_beuk[!,:MWh].-physical_flows_ukbe[!,:MWh])# net flow
mw2pu=findmax(net_physical_flows[!,:MWh])[1];net_physical_flows[!,:MWh]=net_physical_flows[!,:MWh]./mw2pu#into PU=#

#Scheduled commercial exchanges
#"A05" gross
#=CommercialExchange_beuk_gross=_EE.commercial_exchanges(Z1,UK,_start,_end,"A05")
CommercialExchange_ukbe_gross=_EE.commercial_exchanges(UK,Z1,_start,_end,"A05")
CommercialExchange_net=DataFrame(time_stamp=_EE.CommercialExchange_beuk_gross[!,:time_stamp],MW=CommercialExchange_beuk_gross[!,:MW].-CommercialExchange_ukbe_gross[!,:MW])=#


#"A01" day ahead
#=CommercialExchange_ukbe_dayahead=_EE.commercial_exchanges(UK,Z1,_start,_end,"A01")
CommercialExchange_beuk_dayahead=_EE.commercial_exchanges(BE,UK,_start,_end,"A01")=#

#combine prices with bidding strategy
#bidding_prices=_EE.bidding_price(CommercialExchange_beuk_dayahead,BE_balancing_price,BE_day_ahead_price,CommercialExchange_ukbe_dayahead,UK_balancing_price,UK_day_ahead_price,CommercialExchange_beuk_gross,CommercialExchange_ukbe_gross,CommercialExchange_net)
#bidding_prices=_EE.bidding_price_0835(CommercialExchange_beuk_dayahead,BE_balancing_price,BE_day_ahead_price,CommercialExchange_ukbe_dayahead,UK_balancing_price,UK_day_ahead_price,CommercialExchange_beuk_gross,CommercialExchange_ukbe_gross,CommercialExchange_net)

#set net flows to PU
#mw2pu=findmax(CommercialExchange_net[!,:MW])[1];CommercialExchange_net[!,:MW]=CommercialExchange_net[!,:MW]./mw2pu#into PU

# agregated wind generation
wind_gen_be=_EE.agrigated_wind_generation(Z1,_start,_end);unique!(wind_gen_uk,:time_stamp)# Belgium
mw2pu=findmax(wind_gen_be[!,:MWh])[1];wind_gen_be[!,:MWh]=wind_gen_be[!,:MWh]./mw2pu#into PU
wind_gen_uk=_EE.agrigated_wind_generation(UK,_start,_end)# UK
mw2pu=findmax(wind_gen_uk[!,:MWh])[1];wind_gen_uk[!,:MWh]=wind_gen_uk[!,:MWh]./mw2pu#into PU

#FCR amount/cost
ts_regup,ts_regdown=_EE.activated_reserves(Z1,_start,_end)
ts_regup_price,ts_regdown_price=_EE.activated_reserves_prices(Z1,_start,_end)
reg_rez=_EE.summarize_hourly_activated_reserves(ts_regup,ts_regdown,ts_regup_price,ts_regdown_price);unique!(reg_rez,:time_stamp)

#beuk_df=_EE.data_intersection(BE_balancing_price,BE_day_ahead_price,wind_gen_be,UK_balancing_price,UK_day_ahead_price,wind_gen_uk,net_physical_flows)
#beuk_df=_EE.data_intersection2(bidding_prices,wind_gen_be,wind_gen_uk,CommercialExchange_net)
region_summary=innerjoin(wind_gen_uk,UK_day_ahead_price,UK_balancing_price,reg_rez, makeunique=true,on=:time_stamp)
rename!(region_summary, Dict(:MWh => "Wnd_MWh", :EUR => "EUR_da", :EUR_1 => "EUR_id"))
CSV.write("./test/data/cordoba/input/UKdata.csv",region_summary)
