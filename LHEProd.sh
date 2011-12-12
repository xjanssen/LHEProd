#!/bin/bash
########################################################################
#
# author:  Xavier Janssen                                12/09/2011
# purpose: Do LHE production
#
########################################################################

# Basic Config

queue="1nd"
SEEDOffset="10000"
eosBase="root://eoscms//eos/cms/store/lhe/"
WFDir="WorkFlows/"
mkdir -p $WFDir

# ------------------------ print_help --------------------------------------------
print_help()
{

Manual=`echo -e '
LHEProd.sh:
  Script to start/monitor CMS LHE Production on the lxbatch farm

Possibilities:

  LHEProd.sh -inject -tgz <WebWFFile> : 
       Dowload WorkFlow tarball and submit jobs
  LHEProd.sh -submit -dir <WFDir> -lhe <lheID> : 
       Submit lheID WF already dowloaded to WFDir
  LHEProd.sh -status :
       Get Status of all LHE ongoing WF
  LHEProd.sh -close -lhe <lheID> : 
       Close WorkFlow lheID production

Author: Xavier Janssen <xavier.janssen@cern.ch>
        Dec. 2011 

'`

echo "$Manual"
echo
exit

}

# ------------------------  LOAD CFG --------------------------------------------
parse_config()
{

  cfg=$dir'/summary.txt'
  if [ ! -f $cfg ];then
    echo -e "[LHEProd::Config] ERROR: The config file $cfg you specified doesn't exist !"
    exit
  fi

  # Get LHE config line
  grep $lhe $cfg -q &&  cfgline=`(cat $cfg | grep $lhe)` || exit  

  # Decode info
  
  requestID=`(echo $cfgline | awk '{print $1}')`
  Release=`(echo $cfgline | awk '{print $2}' | awk -F'CMSSW_' '{print $2}')`
  Events=`(echo $cfgline | awk '{print $5}')`
  Dataset=`(echo $cfgline | awk '{print $10}')`
  pyCfg=`(echo $cfgline | awk '{print $13}')`
  pyCfg=$dir'/'$pyCfg
  eosnum=`(echo $cfgline | awk '{print $14}')`
  eosDir=$eosBase$eosnum

  EvtJob=`(cat $pyCfg | grep maxEvents | grep "cms.untracked.int32" | awk -F"int32" '{print $2}' | sed 's:(::' | sed 's:)::g' | sed 's: ::g')` 
  nJobs=$(( $Events / $EvtJob )) 

  if [ $nJobMax -gt 0 ] ; then
    nJobs=$nJobMax
  fi
 
  echo
  echo '---------------------------------------'
  echo 'requestID  : '$requestID
  echo 'Release    : '$Release 
  echo '#Events    : '$Events
  echo 'Events/Job : '$EvtJob
  echo '#Jobs      : '$nJobs
  echo 'Dataset    : '$Dataset
  echo 'pyCfg      : '$pyCfg
  echo 'EOS Dir    : '$eosDir
  echo '---------------------------------------'
  echo
  

}

# ------------------------  INJECT WORKFLOW(s) -------------------------------------
inj_lhe()
{
  if [ "$tgz" == "NULL" ] ; then
    echo '[LHEProd::Inject] ERROR tgz not specified'
    exit
  fi

  # Fetch/Unpack WF 
  cd $WFDir 
  wget $tgz
  WFFile=`(echo $tgz | awk -F"/" '{print $NF}')`
  tar xzf $WFFile
  rm $WFFile
  cd ..

  # Get WF tasks and submit
  dir=$WFDir`(echo $WFFile | awk -F".tgz" '{print $1}')`
  AllLHE=`(cat $dir/summary.txt | grep -v "request" | grep -v "Total evts" | awk '{print $1}')`
  for iLHE in $AllLHE ; do
    lhe=$iLHE
    parse_config 
    echo -en "[LHEProd::Inject] INFO : Do you want to submit this WorkFlow ? [y/n] "
    read a
    case $a in
      y) echo "... Submitting ..." ; sub_lhe ;;
      *) echo "... NOT Submitting ..." ;;
    esac  
  done
}

# ------------------------  SUBMIT  JOB(S) --------------------------------------
sub_lhe()
{

  PWD=`pwd`
  BaseDir=`pwd`'/'$dir'/' 
  lockFile=$BaseDir$requestID'.lock'
  actiFile=$BaseDir$requestID'.active'
  if [ -f $lockFile ] ; then
    echo '[LHEProd.sh::Submit] ERROR lockFile exist:' $lockFile
    exit
  fi


  LogDir=$BaseDir'LogFiles_'$requestID
  mkdir -p $LogDir
  submit=$BaseDir$requestID'.sub'
  cp /dev/null $submit
 
  echo '#!/bin/sh'                                          >> $submit
  echo 'let R=$RANDOM%1200+1 ; sleep $R'                    >> $submit
  echo ' '                                                  >> $submit
  echo 'export INPUT=$1 '                                   >> $submit
  echo 'SEED=`(expr $INPUT + '$SEEDOffset')`'               >> $submit
  echo ' '                                                  >> $submit
  echo 'source $HOME/EVAL_SH64 '$Release                    >> $submit
  echo ' '                                                  >> $submit
  echo "cp $PWD/$pyCfg"' temp_${INPUT}.py'                  >> $submit
  echo 'sed -ie  s/1111111/${SEED}/ temp_${INPUT}.py'       >> $submit
  echo 'cmsRun temp_${INPUT}.py'                            >> $submit 
  echo ' '                                                  >> $submit
  echo 'ls -l'                                              >> $submit 
  echo ' '                                                  >> $submit
  echo 'xrdcp -np output.lhe '$eosDir'/'$Dataset'_${SEED}.lhe' >> $submit 
  chmod +x $submit

  iJob=1
  taskID=`(mktemp -p $PWD -t .XXX | awk -F'.' '{print $2}')`
  for (( iJob=$iJobStart ; iJob<=$nJobs ; ++iJob )) ; do  
    echo bsub -q $queue -o $LogDir/$taskID'_'$iJob.out -J $taskID'_'$iJob $submit $iJob
         bsub -q $queue -o $LogDir/$taskID'_'$iJob.out -J $taskID'_'$iJob $submit $iJob
  done 
  cp /dev/null $lockFile
  cp /dev/null $actiFile
  echo $dir $lhe $taskID $nJobs >> $actiFile
} 

# ------------------------  WORFLOW(S) STATUS ------------------------------------
sta_lhe() 
{
  activeLHE=`(find . | grep ".active")`
  for iLHE in $activeLHE ; do
    dir=`(cat $iLHE | awk '{print $1}')`
    lhe=`(cat $iLHE | awk '{print $2}')`
    taskID=`(cat $iLHE | awk '{print $3}')`
    nSubmit=`(cat $iLHE | awk '{print $4}')`    
    lJobs=`(bjobs | grep $taskID | awk '{print $7}' | awk -F "_" '{print $2}')`
    nRun=`(bjobs | grep $taskID | grep "RUN"  | wc | awk '{print $1}')`
    nPend=`(bjobs | grep $taskID | grep "PEND" | wc | awk '{print $1}')`
    parse_config 
    lFiles=`(xrd eoscms dirlist /eos/cms/store/lhe/$eosnum | grep $Dataset | grep eos | awk '{print $5}' | awk -F"/" '{print $NF}')`
    nFiles=`(xrd eoscms dirlist /eos/cms/store/lhe/$eosnum | grep $Dataset | grep eos | awk '{print $5}' | wc | awk '{print $1}' )`
    nFailed=$(($nSubmit - $nRun - $nPend - $nFiles)) 


    echo '  --> Submitted : '$nSubmit' [ Running : '$nRun' / Pending : '$nPend' ]' 
    echo '  --> Finished  : '$nFiles   
    echo '  --> Failed    : '$nFailed 
    echo

    if [ $nFailed -gt 0 ] ; then
      lFailed=""
      for (( iJob=1 ; iJob<=$nSubmit ; ++iJob )) ; do 
        bJobRP=0 
        for jRP in $lJobs ; do      
          if [ "$iJob" == "$jRP" ] ; then
            bJobRP=1
          fi
        done
        if [ $bJobRP -eq 1 ] ; then
          SEED=`(expr $iJob + $SEEDOffset)`
          expFile=$Dataset'_'$SEED'.lhe'
          bJobF=0
          for iFile in $lFiles ; do
            if [ "$iFile" == "$expFile" ] ; then
              bJobF=1
            fi
          done
          if [ $bJobF -eq 0 ] ; then
            lFailed=$lFailed' '$iJob
          fi 
        fi
      done
    fi
    

  done
}

# ------------------------ CLOSE WORFLOW ------------------------------------------
close_lhe()
{
  if [ "$lhe" == "NULL" ] ; then
    echo '[LHEProd::Close] ERROR lhe not specified'
    exit
  fi

  # Find back jobs
  activeLHE=`(find . | grep ".active")`
  Found=0
  for iLHE in $activeLHE ; do
    tmpdir=`(cat $iLHE | awk '{print $1}')`
    tmplhe=`(cat $iLHE | awk '{print $2}')`
    if [ "$tmplhe" == "$lhe" ] ; then
      dir=$tmpdir
      Found=1
      actiFile=$iLHE
    fi
  done

  if [ $Found -eq 0 ] ; then
    echo '[LHEProd::Close] ERROR lhe not in active WF: '$lhe
    exit
  fi

  # Check Status
  echo '[LHEProd::Close] Not Implemented !!!!'
}

#----------------------------------------------------------------------------------
#------------------------ DO EVERYTHING NOW ---------------------------------------
#----------------------------------------------------------------------------------



# Get Options

tgz="NULL"
dir="NULL"
lhe="NULL"
sub=0
sta=0
inj=0
clo=0

nJobMax=0
iJobStart=1

for arg in $* ; do
  case $arg in
    -tgz)    tgz=$2        ; shift ; shift ;;
    -dir)    dir=$2        ; shift ; shift ;;
    -lhe)    lhe=$2        ; shift ; shift ;;
    -inject) inj=1                 ; shift ;;
    -submit) sub=1                 ; shift ;;
    -status) sta=1                 ; shift ;;
    -close)  clo=1                 ; shift ;;
    -njmax)  nJobMax=$2    ; shift ; shift ;;
    -jstart) iJobStart=$2  ; shift ; shift ;;
    -h)      print_help                    ;;
  esac
done

# Do the job

if [ $inj -eq 1 ] ; then
  inj_lhe 
  exit
fi

if [ $sub -eq 1 ] ; then
  parse_config
  sub_lhe
  exit
fi

if [ $sta -eq 1 ] ; then
  sta_lhe
  exit
fi

if [ $clo -eq 1 ] ; then
  close_lhe
  exit
fi

# --- Nothing ?
print_help


