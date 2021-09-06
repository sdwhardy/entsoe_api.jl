my_api_key="cdc2253c-83f9-4017-ad54-83ccc9256faf"
entsoe_url="https://transparency.entsoe.eu/api"
entsoe_api_rest=entsoe_url*"?securityToken="*my_api_key
################################################################################
################################### Document Requests ##########################
function agrigated_wind_generation(Dom,t0,t1)
    entsoe_doc=entsoe_api_rest*"&documentType=A75&processType=A16&psrType=B18&in_Domain="*Dom*"&periodStart="*t0*"&periodEnd="*t1
    response=HTTP.request("GET",entsoe_doc)
    xdoc=parse_xml_response(response.body, "wind_gen_"*Dom*t0*t1)
    wind_gen=parse_AgrigatedGen_MarketDocXML(xdoc)
    return wind_gen
end

function physical_flows(Dom0,Dom1,t0,t1)
    entsoe_doc=entsoe_api_rest*"&documentType=A11&in_Domain="*Dom1*"&out_Domain="*Dom0*"&periodStart="*t0*"&periodEnd="*t1
    response=HTTP.request("GET",entsoe_doc)
    xdoc=parse_xml_response(response.body, "phys_flow_"*Dom0*Dom1*t0*t1)
    physical_flows=parse_PhysicalFlows_MarketDocXML(xdoc)
    return physical_flows
end

function day_ahead_price(Dom,t0,t1)
    entsoe_doc=entsoe_api_rest*"&documentType=A44&in_Domain="*Dom*"&out_Domain="*Dom*"&periodStart="*t0*"&periodEnd="*t1
    response=HTTP.request("GET",entsoe_doc)
    xdoc=parse_xml_response(response.body, "day_ahead_"*Dom*t0*t1)
    day_ahead_price=parse_DayAhead_PriceDoc(xdoc)
    return day_ahead_price
end

function balancing_price(Dom,t0,t1)
    entsoe_doc=entsoe_api_rest*"&documentType=A85&controlArea_Domain="*Dom*"&periodStart="*t0*"&periodEnd="*t1
    response=HTTP.request("GET",entsoe_doc)#get zip doc
    xdoc=parse_zip_attachment(response.body)
    balancing_price=parse_Balancing_PriceDocXML(xdoc)
    return balancing_price
end

function balancing_volume(Dom,t0,t1)
    entsoe_doc=entsoe_api_rest*"&documentType=A86&controlArea_Domain="*Dom*"&periodStart="*t0*"&periodEnd="*t1
    response=HTTP.request("GET",entsoe_doc)
    xdoc=parse_zip_attachment(response.body)
    balancing_volume=parse_Balancing_VolumeDocXML(xdoc)
    return balancing_volume
end

function commercial_exchanges(Dom0,Dom1,t0,t1,ctype)
    entsoe_doc=entsoe_api_rest*"&documentType=A09&in_Domain="*Dom1*"&out_Domain="*Dom0*"&contract_MarketAgreement.Type="*ctype*"&periodStart="*t0*"&periodEnd="*t1
    response=HTTP.request("GET",entsoe_doc)
    xdoc=parse_xml_response(response.body, "market_type"*ctype*Dom0*Dom1)
    CommercialExchange_beuk=parse_CommercialExchangeDocXML(xdoc)
    return CommercialExchange_beuk
end


################################### parsing Responses ##########################
function parse_zip_attachment(content)
    w = open("../entsoe_api/test/data/input/temp.zip","w");#save temporarily
    write(w, content);
    close(w)
    r = ZipFile.Reader("../entsoe_api/test/data/input/temp.zip");
    write("../entsoe_api/test/data/input/fresh_data/"*r.files[1].name, read(r.files[1]))
    xdoc = parse_file("../entsoe_api/test/data/input/fresh_data/"*r.files[1].name)
    return xdoc
end

function parse_xml_response(content,file_name)
    #save as xml doc
    w = open("../entsoe_api/test/data/input/fresh_data/"*file_name*".xml","w");
    write(w, content);close(w)
    #parse xml doc
    xdoc = parse_file("../entsoe_api/test/data/input/fresh_data/"*file_name*".xml")
end


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
########################### Combine multiple length Dataframes ############################

function data_intersection(A,B,C,D,E,F,G)
    nemo_loss_factor=0.02372
    H=DataFrame(time_stamp=G[!,:time_stamp],MWh=G[!,:MWh].*nemo_loss_factor)
    interc=intersect(A[!,:time_stamp],B[!,:time_stamp],C[!,:time_stamp],D[!,:time_stamp],E[!,:time_stamp],F[!,:time_stamp],G[!,:time_stamp])
    df=DataFrame(time_stamp=DateTime[],be_costid=Float64[],be_eumwh=Float64[],be_wind=Float64[],uk_costid=Float64[],uk_eumwh=Float64[],uk_wind=Float64[],losses=Float64[],net_flows=Float64[])

    for t in interc
        push!(df,[t,A[!,:EUR][findfirst(isequal(t),A[!,:time_stamp])], B[!,:EUR][findfirst(isequal(t),B[!,:time_stamp])], C[!,:MWh][findfirst(isequal(t),C[!,:time_stamp])], D[!,:EUR][findfirst(isequal(t),D[!,:time_stamp])], E[!,:EUR][findfirst(isequal(t),E[!,:time_stamp])], F[!,:MWh][findfirst(isequal(t),F[!,:time_stamp])], H[!,:MWh][findfirst(isequal(t),H[!,:time_stamp])], G[!,:MWh][findfirst(isequal(t),G[!,:time_stamp])]])
    end
    return df
end

function data_intersection2(A,B,C,D)
    nemo_loss_factor=0.02372
    E=DataFrame(time_stamp=D[!,:time_stamp],MW=D[!,:MW].*nemo_loss_factor)
    interc=intersect(A[!,:time_stamp],B[!,:time_stamp],C[!,:time_stamp],D[!,:time_stamp],E[!,:time_stamp])
    df=DataFrame(time_stamp=DateTime[],be_costid=Float64[],be_eumwh=Float64[],be_wind=Float64[],uk_costid=Float64[],uk_eumwh=Float64[],uk_wind=Float64[],losses=Float64[],net_flows=Float64[])

    for t in interc
        be_costid=A[!,:be_costid][findfirst(isequal(t),A[!,:time_stamp])]
        be_eumwh=A[!,:be_eumwh][findfirst(isequal(t),A[!,:time_stamp])]
        be_wind=B[!,:MWh][findfirst(isequal(t),B[!,:time_stamp])]
        uk_costid=A[!,:uk_costid][findfirst(isequal(t),A[!,:time_stamp])]
        uk_eumwh=A[!,:uk_eumwh][findfirst(isequal(t),A[!,:time_stamp])]
        uk_wind=C[!,:MWh][findfirst(isequal(t),C[!,:time_stamp])]
        net_flow=D[!,:MW][findfirst(isequal(t),D[!,:time_stamp])]
        losses=E[!,:MW][findfirst(isequal(t),E[!,:time_stamp])]
        push!(df,[t, be_costid, be_eumwh, be_wind, uk_costid, uk_eumwh, uk_wind, losses, net_flow])
    end
    return df
end

function price_volume_intersection(price, volume)
    interc=intersect(volume[!,:time_stamp],price[!,:time_stamp])
    vp=DataFrame(time_stamp=DateTime[], EUR=Float64[], MWh=Float64[]);
    for t in interc
        push!(vp,[t,price[!,:EUR][findfirst(isequal(t),price[!,:time_stamp])], volume[!,:MWh][findfirst(isequal(t),volume[!,:time_stamp])]])
    end
    p=DataFrame(time_stamp=vp[!,:time_stamp],EUR=abs.(vp[!,:EUR]).*sign.(vp[!,:MWh]))
    return p
end

function bidding_price(ce_da0,id_price0,da_price0,ce_da1,id_price1,da_price1,ce_0,ce_1,ce_net)
    interc=intersect(ce_da0[!,:time_stamp],id_price0[!,:time_stamp],da_price0[!,:time_stamp],ce_da1[!,:time_stamp],id_price1[!,:time_stamp],da_price1[!,:time_stamp],ce_0[!,:time_stamp],ce_1[!,:time_stamp],ce_net[!,:time_stamp])
    df=DataFrame(time_stamp=DateTime[],be_costid=Float64[],be_eumwh=Float64[],uk_costid=Float64[],uk_eumwh=Float64[])

    for t in interc
        nf=ce_net[!,:MW][findfirst(isequal(t),ce_net[!,:time_stamp])]
        if (isapprox(nf, 0, atol=1e-1))#no transfer
            da_percent=1;id_percent=0
        elseif (nf>0)#flow towards UK
            nf0=ce_0[!,:MW][findfirst(isequal(t),ce_0[!,:time_stamp])]
            da_percent=ce_da0[!,:MW][findfirst(isequal(t),ce_da0[!,:time_stamp])]*1/abs(nf0)
            id_percent=(abs(nf0)-ce_da0[!,:MW][findfirst(isequal(t),ce_da0[!,:time_stamp])])*1/abs(nf0)
        elseif (nf<0)#flow towards BE
            nf1=ce_1[!,:MW][findfirst(isequal(t),ce_1[!,:time_stamp])]
            da_percent=ce_da1[!,:MW][findfirst(isequal(t),ce_da1[!,:time_stamp])]*1/abs(nf1)
            id_percent=(abs(nf1)-ce_da1[!,:MW][findfirst(isequal(t),ce_da1[!,:time_stamp])])*1/abs(nf1)
        end
        id0=id_price0[!,:EUR][findfirst(isequal(t),id_price0[!,:time_stamp])]
        id1=id_price1[!,:EUR][findfirst(isequal(t),id_price1[!,:time_stamp])]
        bp0=da_price0[!,:EUR][findfirst(isequal(t),da_price0[!,:time_stamp])]*da_percent+id_price0[!,:EUR][findfirst(isequal(t),id_price0[!,:time_stamp])]*id_percent
        bp1=da_price1[!,:EUR][findfirst(isequal(t),da_price1[!,:time_stamp])]*da_percent+id_price1[!,:EUR][findfirst(isequal(t),id_price1[!,:time_stamp])]*id_percent

        push!(df,[t,id0,bp0,id1,bp1])
    end
    return df
end

function bidding_price_0835(ce_da0,id_price0,da_price0,ce_da1,id_price1,da_price1,ce_0,ce_1,ce_net)
    interc=intersect(ce_da0[!,:time_stamp],id_price0[!,:time_stamp],da_price0[!,:time_stamp],ce_da1[!,:time_stamp],id_price1[!,:time_stamp],da_price1[!,:time_stamp],ce_0[!,:time_stamp],ce_1[!,:time_stamp],ce_net[!,:time_stamp])
    df=DataFrame(time_stamp=DateTime[],be_costid=Float64[],be_eumwh=Float64[],uk_costid=Float64[],uk_eumwh=Float64[])

    for t in interc
        nf=ce_net[!,:MW][findfirst(isequal(t),ce_net[!,:time_stamp])]
        da_percent=0.835;id_percent=1-0.835
        id0=id_price0[!,:EUR][findfirst(isequal(t),id_price0[!,:time_stamp])]
        id1=id_price1[!,:EUR][findfirst(isequal(t),id_price1[!,:time_stamp])]
        #bp0=da_price0[!,:EUR][findfirst(isequal(t),da_price0[!,:time_stamp])]*da_percent+id_price0[!,:EUR][findfirst(isequal(t),id_price0[!,:time_stamp])]*id_percent
        #bp1=da_price1[!,:EUR][findfirst(isequal(t),da_price1[!,:time_stamp])]*da_percent+id_price1[!,:EUR][findfirst(isequal(t),id_price1[!,:time_stamp])]*id_percent
        bp0=da_price0[!,:EUR][findfirst(isequal(t),da_price0[!,:time_stamp])]
        bp1=da_price1[!,:EUR][findfirst(isequal(t),da_price1[!,:time_stamp])]

        push!(df,[t,id0,bp0,id1,bp1])
    end
    return df
end


function summarize_hourly_activated_reserves(ts_regup,ts_regdown,ts_regup_price,ts_regdown_price)
    act_res=innerjoin(ts_regup,ts_regdown,ts_regup_price,ts_regdown_price, makeunique=true,on=:time_stamp)
    rename!(act_res, Dict(:MWh => "MWh_up", :EUR => "EUR_up",:MWh_1 => "MWh_dwn", :EUR_1 => "EUR_dwn"))
    actRez=DataFrame(time_stamp=DateTime[], MWh_up=Float64[], EUR_up=Float64[], MWh_dwn=Float64[], EUR_dwn=Float64[])
    h=[]
    for (i,rw) in enumerate(eachrow(act_res))
        if (minute(rw.time_stamp)==0)
            if (i>1)
                push!(actRez,h)
            else
            end
            h=[rw.time_stamp,rw.MWh_up,rw.EUR_up,rw.MWh_dwn,rw.EUR_dwn]
        else
            h[2]=h[2]+rw.MWh_up;h[3]=(h[3]+rw.EUR_up)/2;h[4]=h[4]+rw.MWh_dwn;h[5]=(h[5]+rw.EUR_dwn)/2
        end
    end
    push!(actRez,h)
    return actRez
end
########################### Parse XML to Dataframes ############################
function parse_AgrigatedGen_MarketDocXML(xdoc)
    xroot = root(xdoc)#find root
    timeseries=DataFrame(time_stamp=DateTime[], MWh=Float64[])
    for ts in get_elements_by_tagname(xroot, "TimeSeries")
        for ps in get_elements_by_tagname(ts, "Period")
            strt=DateTime(content(get_elements_by_tagname(get_elements_by_tagname(ps, "timeInterval")[1],"start")[1]),dateformat"yyyy-mm-ddTHH:MMZ")
            res = Minute(parse(Int64,content(get_elements_by_tagname(ps, "resolution")[1])[3:4]))#array (but single entry)
            for pnts in get_elements_by_tagname(ps, "Point")#array
                cst = parse(Float64,content(get_elements_by_tagname(pnts, "quantity")[1]))#array (but single entry)
                pos = parse(Int64,content(get_elements_by_tagname(pnts, "position")[1]))#array (but single entry)
                push!(timeseries,[strt+res*(pos-1),cst])
            end
        end
    end
    sort!(timeseries, :time_stamp)
    unique!(timeseries)
    return timeseries
end

function parse_PhysicalFlows_MarketDocXML(xdoc)
    xroot = root(xdoc)#find root
    timeseries=DataFrame(time_stamp=DateTime[], MWh=Float64[])
    for ts in get_elements_by_tagname(xroot, "TimeSeries")
        for ps in get_elements_by_tagname(ts, "Period")
            strt=DateTime(content(get_elements_by_tagname(get_elements_by_tagname(ps, "timeInterval")[1],"start")[1]),dateformat"yyyy-mm-ddTHH:MMZ")
            res = Minute(parse(Int64,content(get_elements_by_tagname(ps, "resolution")[1])[3:4]))#array (but single entry)
            for pnts in get_elements_by_tagname(ps, "Point")#array
                cst = parse(Float64,content(get_elements_by_tagname(pnts, "quantity")[1]))#array (but single entry)
                pos = parse(Int64,content(get_elements_by_tagname(pnts, "position")[1]))#array (but single entry)
                push!(timeseries,[strt+res*(pos-1),cst])
            end
        end
    end
    sort!(timeseries, :time_stamp)
    unique!(timeseries)
    return timeseries
end

function parse_DayAhead_PriceDoc(xdoc)
    xroot = root(xdoc)#find root
    timeseries=DataFrame(time_stamp=DateTime[], EUR=Float64[])
    currency=""
    for ts in get_elements_by_tagname(xroot, "TimeSeries")
        currency=content(get_elements_by_tagname(ts, "currency_Unit.name")[1])
        for ps in get_elements_by_tagname(ts, "Period")
            strt=DateTime(content(get_elements_by_tagname(get_elements_by_tagname(ps, "timeInterval")[1],"start")[1]),dateformat"yyyy-mm-ddTHH:MMZ")
            res = Minute(parse(Int64,content(get_elements_by_tagname(ps, "resolution")[1])[3:4]))#array (but single entry)
            for pnts in get_elements_by_tagname(ps, "Point")#array
                cst = parse(Float64,content(get_elements_by_tagname(pnts, "price.amount")[1]))#array (but single entry)
                pos = parse(Int64,content(get_elements_by_tagname(pnts, "position")[1]))#array (but single entry)
                push!(timeseries,[strt+res*(pos-1),cst])
            end
        end
    end
    rename!(timeseries,:EUR => currency)
    sort!(timeseries, :time_stamp)
    unique!(timeseries)
    return timeseries
end

#A05=Load, A04=Generation
#A negative price means that energy is received together with a compensation.
function parse_Balancing_PriceDocXML(xdoc)
    xroot = root(xdoc)#find root
    timeseries=DataFrame(time_stamp=DateTime[], EUR=Float64[])
    currency=""
    for ts in get_elements_by_tagname(xroot, "TimeSeries")
        currency=content(get_elements_by_tagname(ts, "currency_Unit.name")[1])
        for ps in get_elements_by_tagname(ts, "Period")
            strt=DateTime(content(get_elements_by_tagname(get_elements_by_tagname(ps, "timeInterval")[1],"start")[1]),dateformat"yyyy-mm-ddTHH:MMZ")
            res = Minute(parse(Int64,content(get_elements_by_tagname(ps, "resolution")[1])[3:4]))#array (but single entry)
            for pnts in get_elements_by_tagname(ps, "Point")#array
                cst = parse(Float64,content(get_elements_by_tagname(pnts, "imbalance_Price.amount")[1]))#array (but single entry)
                pos = parse(Int64,content(get_elements_by_tagname(pnts, "position")[1]))#array (but single entry)
                push!(timeseries,[strt+res*(pos-1),cst])
            end
        end
    end
    rename!(timeseries,:EUR => currency)
    sort!(timeseries, :time_stamp)
    unique!(timeseries)
    return timeseries
end

function parse_Balancing_VolumeDocXML(xdoc)
    xroot = root(xdoc)#find root
    timeseries=DataFrame(time_stamp=DateTime[], MWh=Float64[])
    for ts in get_elements_by_tagname(xroot, "TimeSeries")
        fd=content(get_elements_by_tagname(ts, "flowDirection.direction")[1])=="A01" ? 1 : -1
        #fd=1
        for ps in get_elements_by_tagname(ts, "Period")
            strt=DateTime(content(get_elements_by_tagname(get_elements_by_tagname(ps, "timeInterval")[1],"start")[1]),dateformat"yyyy-mm-ddTHH:MMZ")
            res = Minute(parse(Int64,content(get_elements_by_tagname(ps, "resolution")[1])[3:4]))#array (but single entry)
            for pnts in get_elements_by_tagname(ps, "Point")#array
                pos = parse(Int64,content(get_elements_by_tagname(pnts, "position")[1]))#array (but single entry)
                qnt = parse(Int64,content(get_elements_by_tagname(pnts, "quantity")[1]))#array (but single entry)
                push!(timeseries,[strt+res*(pos-1),qnt*fd])
            end
        end
    end
    sort!(timeseries, :time_stamp)
    return timeseries
end

function parse_CommercialExchangeDocXML(xdoc)
    xroot = root(xdoc)#find root
    timeseries=DataFrame(time_stamp=DateTime[], MW=Float64[])
    for ts in get_elements_by_tagname(xroot, "TimeSeries")
        for ps in get_elements_by_tagname(ts, "Period")
            strt=DateTime(content(get_elements_by_tagname(get_elements_by_tagname(ps, "timeInterval")[1],"start")[1]),dateformat"yyyy-mm-ddTHH:MMZ")
            res = Minute(parse(Int64,content(get_elements_by_tagname(ps, "resolution")[1])[3:4]))#array (but single entry)
            for pnts in get_elements_by_tagname(ps, "Point")#array
                cst = parse(Float64,content(get_elements_by_tagname(pnts, "quantity")[1]))#array (but single entry)
                pos = parse(Int64,content(get_elements_by_tagname(pnts, "position")[1]))#array (but single entry)
                push!(timeseries,[strt+res*(pos-1),cst])
            end
        end
    end
    sort!(timeseries, :time_stamp)
    unique!(timeseries)
    return timeseries
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
