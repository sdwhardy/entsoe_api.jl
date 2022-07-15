using HTTP
using Dates
using DataFrames
using Plots
using CSV
using LightXML
using Indicators
#############################################################################################################################
my_api_key="cdc2253c-83f9-4017-ad54-83ccc9256faf" #****************************** Add your API key here *********************
entsoe_url="https://transparency.entsoe.eu/api"
entsoe_api_rest=entsoe_url*"?securityToken="*my_api_key
################################################################################
_yr="2018"
_scenario="0"
_country="DE"
_start=_yr*"01010000";_end=_yr*"12312300";GBP2EUR=1.15
#Z1="10YNL----------L"#Netherlands
#Z1="10YBE----------2"#Belgium
Z1="10Y1001A1001A82H"#Germany

#FCR amount/cost
ts_regup,ts_regdown=activated_reserves(Z1,_start,_end)
ts_regup_price,ts_regdown_price=activated_reserves_prices(Z1,_start,_end)
reg_rez=summarize_hourly_activated_reserves(ts_regup,ts_regdown,ts_regup_price,ts_regdown_price);unique!(reg_rez,:time_stamp)

for (i,updwn) in enumerate(reg_rez["MWh_up"].-reg_rez["MWh_dwn"]); if (updwn>=0);
        reg_rez.MWh_up[i]=updwn;reg_rez.EUR_dwn[i]=0;reg_rez.MWh_dwn[i]=0 else;
        reg_rez.MWh_dwn[i]=abs(updwn);reg_rez.EUR_up[i]=0;reg_rez.MWh_up[i]=0;end;end
#############################################################################################################################

################################################################ Functions ##################################################
# *******************location of folders will have to be updated****************
function activated_reserves(Dom,t0,t1)
    entsoe_doc=entsoe_api_rest*"&documentType=A83&businessType=A96&controlArea_Domain="*Dom*"&periodStart="*t0*"&periodEnd="*t1
    response=HTTP.request("GET",entsoe_doc)#get zip doc
    xdoc=parse_xml_response(response.body, "activated_reserves_"*Dom*t0*t1)
    ts_regup,ts_regdown=activated_reserves_Doc(xdoc)
    return ts_regup,ts_regdown
end

function activated_reserves_prices(Dom,t0,t1)
    entsoe_doc=entsoe_api_rest*"&documentType=A84&businessType=A96&controlArea_Domain="*Dom*"&periodStart="*t0*"&periodEnd="*t1
    response=HTTP.request("GET",entsoe_doc)#get zip doc
    xdoc=parse_xml_response(response.body, "activated_reserves_prices"*Dom*t0*t1)
    ts_pregup,ts_pregdown=activated_reserves_PriceDoc(xdoc)
    return ts_pregup,ts_pregdown
end


function parse_xml_response(content,file_name)
    #save as xml doc
    w = open("../entsoe_api/test/data/input/fresh_data/"*file_name*".xml","w");# *******************location of folder will have to be updated****************
    write(w, content);close(w)
    #parse xml doc
    xdoc = parse_file("../entsoe_api/test/data/input/fresh_data/"*file_name*".xml")# *******************location of folder will have to be updated****************
end

function activated_reserves_Doc(xdoc)
    xroot = root(xdoc)#find root
    timeseriesA01=DataFrame(time_stamp=DateTime[], MWh=Float64[])
    timeseriesA02=DataFrame(time_stamp=DateTime[], MWh=Float64[])
    for ts in get_elements_by_tagname(xroot, "TimeSeries")
        direction=content(get_elements_by_tagname(ts, "flowDirection.direction")[1])
        for ps in get_elements_by_tagname(ts, "Period")
            strt=DateTime(content(get_elements_by_tagname(get_elements_by_tagname(ps, "timeInterval")[1],"start")[1]),dateformat"yyyy-mm-ddTHH:MMZ")
            res = Minute(parse(Int64,content(get_elements_by_tagname(ps, "resolution")[1])[3:4]))#array (but single entry)
            for pnts in get_elements_by_tagname(ps, "Point")#array
                cst=parse(Float64,content(get_elements_by_tagname(pnts, "quantity")[1]))#array (but single entry)
                pos = parse(Int64,content(get_elements_by_tagname(pnts, "position")[1]))#array (but single entry)
                if (direction=="A01")
                    push!(timeseriesA01,[strt+res*(pos-1),cst])
                elseif (direction=="A02")
                    push!(timeseriesA02,[strt+res*(pos-1),cst])
                end
            end
        end
    end
    sort!(timeseriesA01, :time_stamp)
    unique!(timeseriesA01)
    sort!(timeseriesA02, :time_stamp)
    unique!(timeseriesA02)
    return timeseriesA01,timeseriesA02
end

function activated_reserves_PriceDoc(xdoc)
    xroot = root(xdoc)#find root
    timeseriesA01=DataFrame(time_stamp=DateTime[], EUR=Float64[])
    timeseriesA02=DataFrame(time_stamp=DateTime[], EUR=Float64[])
    for ts in get_elements_by_tagname(xroot, "TimeSeries")
        direction=content(get_elements_by_tagname(ts, "flowDirection.direction")[1])
        for ps in get_elements_by_tagname(ts, "Period")
            strt=DateTime(content(get_elements_by_tagname(get_elements_by_tagname(ps, "timeInterval")[1],"start")[1]),dateformat"yyyy-mm-ddTHH:MMZ")
            res = Minute(parse(Int64,content(get_elements_by_tagname(ps, "resolution")[1])[3:4]))#array (but single entry)
            for pnts in get_elements_by_tagname(ps, "Point")#array
                cst=parse(Float64,content(get_elements_by_tagname(pnts, "activation_Price.amount")[1]))#array (but single entry)
                pos = parse(Int64,content(get_elements_by_tagname(pnts, "position")[1]))#array (but single entry)
                if (direction=="A01")
                    push!(timeseriesA01,[strt+res*(pos-1),cst])
                elseif (direction=="A02")
                    push!(timeseriesA02,[strt+res*(pos-1),cst])
                end
            end
        end
    end
    sort!(timeseriesA01, :time_stamp)
    unique!(timeseriesA01)
    sort!(timeseriesA02, :time_stamp)
    unique!(timeseriesA02)
    return timeseriesA01,timeseriesA02
end

function procured_reserves_PriceDoc(xdoc)
    xroot = root(xdoc)#find root
    timeseriesA01=DataFrame(time_stamp=DateTime[], EUR=Float64[])
    timeseriesA02=DataFrame(time_stamp=DateTime[], EUR=Float64[])
    for ts in get_elements_by_tagname(xroot, "TimeSeries")
        direction=content(get_elements_by_tagname(ts, "flowDirection.direction")[1])
        for ps in get_elements_by_tagname(ts, "Period")
            strt=DateTime(content(get_elements_by_tagname(get_elements_by_tagname(ps, "timeInterval")[1],"start")[1]),dateformat"yyyy-mm-ddTHH:MMZ")
            res = Minute(parse(Int64,content(get_elements_by_tagname(ps, "resolution")[1])[3:4]))#array (but single entry)
            for pnts in get_elements_by_tagname(ps, "Point")#array
                cst=parse(Float64,content(get_elements_by_tagname(pnts, "procurement_Price.amount")[1]))#array (but single entry)
                pos = parse(Int64,content(get_elements_by_tagname(pnts, "position")[1]))#array (but single entry)
                if (direction=="A01")
                    push!(timeseriesA01,[strt+res*(pos-1),cst])
                elseif (direction=="A02")
                    push!(timeseriesA02,[strt+res*(pos-1),cst])
                end
            end
        end
    end
    sort!(timeseriesA01, :time_stamp)
    unique!(timeseriesA01)
    sort!(timeseriesA02, :time_stamp)
    unique!(timeseriesA02)
    return timeseriesA01,timeseriesA02
end


function procured_reserves_prices(Dom,t0s,t1s)
    ts_pregup=DataFrame(time_stamp=[],EUR=[]);ts_pregdown=DataFrame(time_stamp=[],EUR=[]);
    t0t=DateTime(t0s,dateformat"yyyymmddHHMM")
    t1t=DateTime(t1s,dateformat"yyyymmddHHMM")
    for t0=t0t:Day(1):t1t
        t1=t0+Day(1)
        t0=string(t0);t1=string(t1)
        t0=string(t0[1:4]*t0[6:7]*t0[9:10]*t0[12:13]*t0[15:16]);t1=string(t1[1:4]*t1[6:7]*t1[9:10]*t1[12:13]*t1[15:16])
        entsoe_doc=entsoe_api_rest*"&documentType=A89&type_MarketAgreement.Type=A01&businessType=A96&controlArea_Domain="*Dom*"&periodStart="*t0*"&periodEnd="*t1
        response=HTTP.request("GET",entsoe_doc)#get zip doc
        xdoc=parse_xml_response(response.body, "activated_reserves_prices"*Dom*t0*t1)
        ts_pregup_temp,ts_pregdown_temp=activated_reserves_PriceDoc(xdoc)
        ts_pregup=vcat(ts_pregup,ts_pregup_temp)
        ts_pregdown=vcat(ts_pregdown,ts_pregdown_temp)
    end
    return ts_pregup,ts_pregdown
end

function summarize_hourly_activated_reserves(ts_regup,ts_regdown,ts_regup_price,ts_regdown_price)
    act_res=innerjoin(ts_regup,ts_regdown,ts_regup_price,ts_regdown_price, makeunique=true,on=:time_stamp)
    rename!(act_res, Dict(:MWh => "MWh_up", :EUR => "EUR_up",:MWh_1 => "MWh_dwn", :EUR_1 => "EUR_dwn"))
    actRez=DataFrame(time_stamp=DateTime[], MWh_up=Float64[], EUR_up=Float64[], MWh_dwn=Float64[], EUR_dwn=Float64[])
    h=[]
    for (i,rw) in enumerate(eachrow(act_res))
        if (minute(rw.time_stamp)==0)
            h=[rw.time_stamp,rw.MWh_up,rw.EUR_up,rw.MWh_dwn,rw.EUR_dwn]
            push!(actRez,h)
        else
        end
    end
    push!(actRez,h)
    return actRez
end
