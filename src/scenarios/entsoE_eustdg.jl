######################################
function entso_scenarios_eustdg(eustdg_datas,countries,_scs,_yrs)
    for (_yr, zs_data) in eustdg_datas;
        for (i, _sc) in enumerate(_scs);
            aaa=[]
            for country in countries
                a=filter(row -> row.time_stamp in eustdg_datas[_yr][string(_sc[2])][country].time_stamp, eustdg_datas[_yr][string(_sc[1])][country])
                filter!(row -> row.time_stamp in eustdg_datas[_yr][string(_sc[3])][country].time_stamp, a);unique!(a,:time_stamp)
                b=filter(row -> row.time_stamp in a.time_stamp, eustdg_datas[_yr][string(_sc[2])][country]);unique!(b,:time_stamp)
                c=filter(row -> row.time_stamp in a.time_stamp, eustdg_datas[_yr][string(_sc[3])][country]);unique!(c,:time_stamp)
                a["EUR_da"*country]=a["EUR_da"*country].*(5/25).+b["EUR_da"*country].*(10/25).+c["EUR_da"*country].*(10/25)
                push!(aaa,a)
            end
            d=innerjoin(aaa[1],aaa[2], makeunique=true,on=:time_stamp)
            d=innerjoin(d,aaa[3], makeunique=true,on=:time_stamp)
            if (i==1); scen="EU";elseif (i==2);scen="ST";else;scen="DG";end
            CSV.write("./test/data/input/scenarios/scenario_"*scen*_yr*".csv",d)
        end
    end
end

######################################
_yr="2019";zs_data=eustdg_datas[_yr]
_sc=_scs[1]
#EU: 0, 1, 4
#ST: 0, 2, 5
#DG: 0, 3, 6
function entso_convex_scenarios_eustdg(eustdg_datas,countries,_scs,_yrs)
    for (bs_yr, zs_data) in eustdg_datas;
        for (i, _sc) in enumerate(_scs);
            for j=1:1:3
                a0=eustdg_datas[bs_yr][string(_sc[j])][countries[1]]
                b0=eustdg_datas[bs_yr][string(_sc[j])][countries[2]]
                c0=eustdg_datas[bs_yr][string(_sc[j])][countries[3]]
                a0=filter(row -> row.time_stamp in b0.time_stamp,a0)
                a0=filter(row -> row.time_stamp in c0.time_stamp,a0)
                b0=filter(row -> row.time_stamp in a0.time_stamp,b0)
                c0=filter(row -> row.time_stamp in a0.time_stamp,c0)
                a0=innerjoin(a0,b0, makeunique=true,on=:time_stamp)
                a0=innerjoin(a0,c0, makeunique=true,on=:time_stamp)
                if (i==1); scen="EU";elseif (i==2);scen="ST";else;scen="DG";end
                if (j==1); _yr="2019";elseif (j==2);_yr="2030";else;_yr="2040";end
                CSV.write("./test/data/input/scenarios/convex/"*scen*bs_yr*_yr*".csv",a0)
            end
        end
    end
end


function entso_scenarios_tss_eustdg(countries,_scs,_yrs)
    zs_datas=Dict{String,Any}()
    for _yr in _yrs
        push!(zs_datas,_yr=>Dict{String,Any}())
        for _sc in _scs
            for _s in _sc
                push!(zs_datas[_yr],_s=>Dict{String,Any}())
                for country in countries
                df=CSV.read("./test/data/input/scenarios/"*_yr*"/"*country*"_"*_s*".csv", DataFrames.DataFrame)
                if (haskey(df,"Column1"))
                    df=select!(df, Not(:Column1))
                end
                if (haskey(df,"Day-ahead Price [GBP/MWh]"))
                    df=select!(df, Not("Day-ahead Price [GBP/MWh]"))
                end

                colnames = ["time_stamp","Wnd_MWh"*country,"EUR_da"*country]
                names!(df, Symbol.(colnames))
                push!(zs_datas[_yr][_s],country=>df)
            end;
        end;end;
    end
    return zs_datas
end
#####################################
