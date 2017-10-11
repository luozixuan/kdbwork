system "c 3000 3000";


SYMLIST:`USDJPY;
SYMPERIODS:1 5 15 30;

@[Rcmd;"library(TTR)";{.ceplog.fatal[msg:"Achelous theta R TTR library failure:",x;0b;`TBD;"TBD"];'msg}];
@[Rcmd;"library(quantmod)";{.ceplog.fatal[msg:"Achelous theta R quantmod library failure:",x;0b;`TBD;"TBD"];'msg}];
@[Rcmd;"library(xts)";{.ceplog.fatal[msg:"Achelous theta R xts library failure:",x;0b;`TBD;"TBD"];'msg}];
.alpha.dataPath:"";
.alpha.flagPath:"";

.priceInd.dobySYM:{[alphaSYM]
    lasttwo:`time$-2#(select from .priceInd.hotPrice where sym=alphaSYM)`timestamp;
    if[.priceInd.ifUpOHLC[lasttwo];
        .priceInd.ohlcTab:.priceInd.updateOHLC[alphaSYM];
        uptar:.priceInd.ifUpInd[lasttwo;SYMPERIODS];
        .priceInd.upsertInd[;alphaSYM;.priceInd.ohlcTab] each ((SYMPERIODS) where uptar);
        .alpha.signal:.priceInd.updateSignal[alphaSYM];
    ];
    if[not .alpha.signal~0i;
        outputMessage:([] time:.z.T; sym:first alphaSYM;
        universalID:.uid.getuid[];
        timestamp:.z.p;
        signal:.alpha.signal;
        modelID:`AlphaCEP;
        );
        .ceplog.info[-3!outputMessage;1b;`TBD;"TBD"];
        .cep.pub[`AlphaCEP; outputMessage; tabdata`universalID]
        ];
    };


.alpha.updMarketBook:{[tabname;tabdata]
    .alpha.signal:0i;
    internalTabUpdate:select sym,time,timestamp, midPrice:0.5*bidPrices+askPrices from select timestamp, first each bidPrices, first each askPrices from tabdata where venue=VENUE,sym in SYMLIST;
    symre:raze select distinct sym from internalTabUpdate;
    if[0=count internalTabUpdate; :(::)];
    .priceInd.hotPrice:`timestamp xasc (delete from `.priceInd.hotPrice where time < .z.P - MAXLEN,sym in symre)
            upsert internalTabUpdate;
    {.priceInd.dobySYM[x]} each symre;
    };


//Should do this before subscribe to TP
//Before subscrbing to TP , query the RDB will not affect the TP itself
.priceInd.init:{
    .priceInd.initTabs[];
    .priceInd.buildConfig[];
    .priceInd.ohlcs:.priceInd.getData[.alpha.dataPath];
    iniflag:.priceInd.getflag[.alpha.flagPath];
    if[iniflag;.priceInd.hotdata:.priceInd.qureyRDB[VENUE;SYMLIST;max SYMPERIODS]];    
    };

.priceInd.initTabs:{
    .priceInd.indcatorTab:([period:`symbol$();sym:`symbol$()]BBand:`float$();CCI:`float$();CMO:`float$();connorsRSI:`float$();RSI:`float$();stoch:`float$();cumRSI:`float$();lastupdate:`timestamp$());
    .priceInd.ohlcTab:([]sym:`symbol$();timestamp:`timestamp$();open:`float$();close:`float$();high:`float$();low:`float$());
    .priceInd.hotPrice:([]time:`time$();timestamp:`timestamp$();sym:`symbol$(); midPrice:`float$());
    .priceInd.indParams:([sym:`symbol$()];BBand:`float$();CCI:`float$();CMO:`float$();connorsRSI:`float$();RSI:`float$();stoch:`float$();cumRSI:`float$());
    .priceInd.weights:([sym:`symbol$()];BBand:`float$();CCI:`float$();CMO:`float$();connorsRSI:`float$();RSI:`float$();stoch:`float$();cumRSI:`float$());
    };

.priceInd.buildConfig:{

    };

//Flag file should be created by job system after start cep every Monday
//And delete the flag after terminate the cep every Staurday
.priceInd.getflag:{[fullpath]
    targetpath:hsym `$fullPath;
    //TODO add error handling
    @[`flagFile in key targetpath;:1b;:0b]
    };

//Get the ohlc data at the begging to calculate the indicator
.priceInd.getData:{[fullpath]
    targetpath:hsym `$fullPath;
    predata:@[get;targetpath;{.ceplog.error["Could not get initial data set at ",(-3!.z.P);1b;`TBD;"TBD"];'ErrorOnGetPreData}];
    if[(.z.P - 1#predata`timestamp) <  25*01:00:00;.ceplog.error["Could not get enough data set at ",(-3!.z.P);1b;`TBD;"TBD"];'ErrorOnGetPreData];
    :predata
    };

//  Get last 30mins data
/** if process is down more than 30mins, should manually recover data and save on disk then start the alpha cep
/** when process is restart 
/** query the rdb for the latest 30mins
/** should do this before subscribe to TP
/** within the 30 mins after the EOD, it could not query the RDB or HDB(should modify after OneHDB)
.priceInd.qureyRDB:{[venue;symlist;LASTLOOK]
    startTime:string (.z.P - LASTLOOK);
    cmd:"select time,timestamp, midPrice:0.5*bidPrices+askPrices from select timestamp, first each bidPrices, first each askPrices from tabdata where timestamp >=",startTime, " venue=",string venue," ,sym in ", string symlist;
    res:.cep.sendQuery[`RDB_HANDLER;cmd];
    :res
    };


.priceInd.ohlcfunc:{
    `open`high`low`close !(first;max;min;last)@\:x
    };

.priceInd.ifUpOHLC:{[times]
    if[(`ss$times[1]=59i) and (`ss$times[0]=0i):1b];
    :0b
    };

.priceInd.ifUpInd:{[times;periods]
    ret:enlist 1b;
    minute:`uu$times[0];
    ret:ret,{$[((minute mod x)=0i);1b;0b]} each periods;
    :ret
    };

//Every minute should update the OHLC-1min
//insert new minute and delete oldest one
.priceInd.updateOHLC:{[tarsym]
    t:select from .priceInd.hotprice where sym=tarsym,time >=  .z.P - 00:02:00;
    up_t:-1#(exec first timestamp, sym,.priceInd.ohlcfunc midPrice by time.minute from t);
    up_t:update timestamp: 00:01 + 0D00:01 xbar timestamp  from up_t;
    `.priceInd.ohlcTab insert up_t;
    :delete from (`timestamp`sym xasc .priceInd.ohlcTab) where sym=tarsym,i=0;
    };


.priceInd.upsertInd:{[per;tarsym;oneOHLC]
    tab1:select from oneOHLC where sym=tarsym;
    tab2:select open:first open,high:max high,low:min low,close:last close by timestamp from update timestamp:(per*00:01) + (per*0D00:01) xbar timestamp from  tab1;
    indcators:.priceInd.calcInd[tab2];
    };


.priceInd.calcInd:{[dataset;cur]
    rsi:.priceInd.RSI[dataset;((.priceInd.indParams)cur)`RSI];
    bband:.priceInd.BB[dataset;((.priceInd.indParams)cur)`BBand;2]; //TODO parameterize the 2
    cci:.priceInd.CCI[dataset;((.priceInd.indParams)cur)`CCI];
    cmo:.priceInd.CMO[dataset;((.priceInd.indParams)cur)`CMO];
    conRSI:.priceInd.connorsRSI[dataset;((.priceInd.indParams)cur)`connorsRSI;2]; //TODO parameterize the 2
    stoch:.priceInd.stoch[dataset;((.priceInd.indParams)cur)`stoch];
    //TODO sumRSI
    };


.priceInd.updateSignal:{[]

    };


//save ohlc data
//the reason we save the ohlc is there are some indicators needs long period of data
.priceInd.saveohlc:{[fullpath]
    targetpath:hsym `$fullPath;
    @[save;targetpath;{msg:"Could not save data because of",x;.ceplog.error[msg," at ",(-3!.z.P);1b;`TBD;"TBD"]}];
    };

.priceInd.connorsRSI:{[rawdata;nRSI;nStreak]
    rsi:.priceInd.RSI[rawdata;nRSI];
    Rset["price";rawdata`close];Rset["nStreak";nStreak];
    Rcmd["streaktmp<-computeStreak(price)"];Rcmd["streakva<-RSI(streaktmp,nStreak)"];
    streakV:Rget["streakva"];
    Rcmd["pecentsva<-round(runPercentRank(x = diff(log(price)),n = 100, cumulative = FALSE, exact.multiplier = 1) * 100)"];
    percents:Rget["pecentsva"];
    //TODO sumRSI
    :(-1#(rsi+streakV+percents)) % 3
    };



.priceInd.CCI:{[rawdata;nCCI]    
    Rset["nCCI";nCCI];
    Rset["Data";rawdata];Rcmd["ccires<-CCI(HLC(Data),n=nCCI)"];
    res:Rget["ccires"];
    :-1#res
    };

.priceInd.CMO:{[rawdata;nCMO]    
    Rset["nCMO";nCMO];
    Rset["Data";rawdata];Rcmd["cmores<-CMO(Cl(Data),n=nCMO)"];
    res:Rget["cmores"];
    :-1#res
    };


.priceInd.RSI:{[rawdata;nRSI]    
    Rset["nRSI";nRSI];
    Rset["Data";rawdata];Rcmd["rsires<-RSI(Cl(Data),n=nRSI)"];
    res:Rget["rsires"];
    :-1#res
    };

.priceInd.BB:{[rawdata;nBB;sd]    
    Rset["nBB";nBB];Rset["sd";sd];
    Rset["Data";rawdata];Rcmd["BBres<-BBands(Cl(Data),n=nBB)"];
    res:Rget["BBres"];    
    :-1#(res[4]*100)
    };

.priceInd.stoch:{[rawdata;nfastk]    
    Rset["nfastk";nfastk];
    Rset["Data";rawdata];Rcmd["stores<-stoch(HLC(Data),nFastK=nfastk)"];
    res:Rget["stores"];    
    :-1#(res*100)
    };
