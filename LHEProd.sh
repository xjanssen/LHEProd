#!/bin/bash
########################################################################
#
# author:  Xavier Janssen                                12/09/2011
# purpose: Do LHE production
#
########################################################################

# Basic Config

email="cmslheprod@gmail.com"
SEEDOffset="10000"
WFDir="WorkFlows/"
mkdir -p $WFDir

# Get Site
Site=`(uname -a | awk '{print $2}' | awk -F'.' '{print $2}')`

# Site Config: CERN
if   [ "$Site" == "cern" ] ; then

  #... basix 
  #queue="cmst0"
  queue="2nd"
  chkqueue='1nd'
  eosBase="root://eoscms//eos/cms/store/lhe/"
  LsfOutDir="LsfOutDir"
  mkdir -p $LsfOutDir
  WorkArea=$HOME"/scratch0/LHEProdWorkArea/"
  mkdir -p $WorkArea

  BJOBS='ssh lxplus bjobs 2> /dev/null'
  BJOBS='bjobs '
  BSUB='ssh lxplus bsub'
  BSUB='bsub'

  #... sync
  source /afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh
  globusDir=`pwd`'/.globus'
  fnalsrm='srm://cmseos.fnal.gov:8443/srm/v2/server?SFN='
  fnaleos='/eos/uscms/store/lhe'

# Site Config: FNAL
elif [ "$Site" == "fnal" ] ; then

  #fnaluser='xjanssen'
  fnaluser=`(klist 2> /dev/null | grep "Default principal:" | awk -F': ' '{print $2}' | awk -F'@' '{print $1}')`
  eosBase="root://cmseos:1094//eos/uscms/store/lhe/"
  WorkArea="/storage/local/data1/cmsdataops/lhe/LHEProdWorkArea/"
  mkdir -p $WorkArea

# Site Config: Unknown ?
else

  echo "[LHEProd] ERROR: Unknown Site : " $Site 
  exit

fi

# ------------------------ print_help --------------------------------------------
print_help()
{

Manual=`echo -e '
LHEProd.sh:
  Script to start/monitor CMS LHE Production on the lxbatch farm

Possibilities:

  LHEProd.sh -inject -tgz <WebWFFile> : 
       Dowload WorkFlow tarball and submit jobs

  LHEProd.sh -submit -dir <WFDir> -lhe <lheID> [-njmax <nJobs>]: 
       Submit lheID WF already dowloaded to WFDir
       -njmax <nJobs>: start nJobs

  LHEProd.sh -extsub <Site> -dir <WFDir> -lhe <lheID> :
       Register lheID WF as being submitted at another site <Site>

  LHEProd.sh -resub  [-lhe <lheID>] 
       Resubmit Failed jobs for all LHE ongoing WF or lheID WF   

  LHEProd.sh -addjob <#job> -lhe <lheID>
       Submit <#job> extra jobs for <lheID> WF

  LHEProd.sh -kill -lhe <lheID>

  LHEProd.sh -status [-lhe <lheID>] [-fjlist]:
       Get Status of all LHE ongoing WF or of lheID WF
       -fjlist : Get List of failed jobs (be patient)

  LHEProd.sh -sync -lhe <lheID>
       Copy remote WF output files back to CERN EOS

  LHEProd.sh -chknevt [-lhe <lheID>]
       Check # produced events for all LHE ongoing WF or lheID WF
       (This is taking quite long time .... )

  LHEProd.sh -cleanlog
       Clean AFS temporary logfile location 

  LHEProd.sh -close -lhe <lheID> : 
       Close WorkFlow lheID production if finished

Author: Xavier Janssen <xavier.janssen@cern.ch>
        Dec. 2011 

'`

echo "$Manual"
echo
exit

}

# ------------------------ get first seed -------------------------------------
get_first_seed()
{
  SEEDOffset="10000"
  touch FirstSeedList.txt
# FirstSeed=$RANDOM
  FirstSeed=`python getrandom.py`
  while grep -q "$FirstSeed" FirstSeedList.txt ; do
#   FirstSeed=$RANDOM
    FirstSeed=`python getrandom.py`
  done  
  echo $FirstSeed >> FirstSeedList.txt
  SEEDOffset=`(expr $FirstSeed + $SEEDOffset)`
  echo $FirstSeed 
  echo $SEEDOffset
}

# ------------------------ chk_afs --------------------------------------------
chk_afs()
{

  if   [ "$Site" == "cern" ] ; then

    #... check AFS token

    klist -5 &> /dev/null
    if [ $? -ne  0 ] ; then 
      echo "No AFS token, please renew it !" 
      exit
    fi

    unixuser=`whoami`
    afsuser=`(klist -5 2> /dev/null | grep "Default principal:" | awk -F': ' '{print $2}' | awk -F'@' '{print $1}')` 

    if [ "$unixuser" !=  "$afsuser" ] ; then
      echo "AFS token not from unix user !"
      exit 
    fi  
  fi
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
  grep $lhein $cfg -q &&  cfgline=`(cat $cfg | grep $lhein)` || exit  
  #echo $cfgline

  # Decode info
  
  requestID=`(echo $cfgline | awk '{print $1}')`
  Release=`(echo $cfgline | awk '{print $2}' | awk -F'CMSSW_' '{print $2}')`
  Events=`(echo $cfgline | awk '{print $5}')`
  Dataset=`(echo $cfgline | awk '{print $10}')`
  pyCfg=`(echo $cfgline | awk '{print $13}')`
  eosnum=`(echo $cfgline | awk '{print $14}')`
  eosDir=$eosBase$eosnum

  EvtJob=`(cat $dir'/'$pyCfg | grep maxEvents | grep "cms.untracked.int32" | awk -F"int32" '{print $2}' | sed 's:(::' | sed 's:)::g' | sed 's: ::g')` 
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
  echo 'pyCfg      : '$dir'/'$pyCfg
  echo 'EOS Dir    : '$eosDir
  echo '---------------------------------------'
  echo
  
  return

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
    lhein=$iLHE
    parse_config 
    echo -en "[LHEProd::Inject] INFO : Do you want to submit this WorkFlow ? [y/n] "
    read a
    case $a in
      y) echo "... Submitting ..." ; sub_lhe ; continue ;;
      *) echo "... NOT Submitting ..." ;;
    esac  
#    if [ "$Site" == "cern" ] ; then
#      echo -en "[LHEProd::Inject] INFO : Do you want to Register this WorkFlow at FNAL ? [y/n] " 
#      read a
#      case $a in
#        y) echo "... Registering ..." ; extsub="fnal" ; extsub_lhe ;;
#        *) echo "... NOT Registering ..." ;;
#      esac  
#    fi

  done
}

# ------------------------  SUBMIT  JOB(S) --------------------------------------
sub_lhe()
{

# chk_afs
  
  PWD=`pwd`
  BaseDir=`pwd`'/'$dir 
  lockFile=$BaseDir'/'$requestID'.lock'
  actiFile=$BaseDir'/'$requestID'.active'
  echo $lockFile
  if [ -f $lockFile ] ; then
    echo '[LHEProd.sh::Submit] ERROR lockFile exist:' $lockFile
    exit
  fi

  echo -en "[LHEProd::Submit] INFO : Do you want to submit this WorkFlow ? [y/n] "
  read a
  case $a in
    y) echo "... Submitting ..." ;;
    *) exit ;;
  esac 

  get_first_seed
  LogDir=$BaseDir'/LogFiles_'$requestID'/'
  mkdir -p $LogDir
  WFWorkArea=$WorkArea$requestID'/'
  mkdir -p $WFWorkArea
  submit=$WFWorkArea$requestID'.sub'
  cp /dev/null $submit
  cp $dir'/'$pyCfg $WFWorkArea

  subHOST=`hostname`
 
  echo '#!/bin/sh'                                          >> $submit
  echo 'let R=$RANDOM%1200+1 ; sleep $R'                    >> $submit
  echo ' '                                                  >> $submit
  echo 'export INPUT=$1 '                                   >> $submit
  echo 'SEED=`(expr $INPUT + '$SEEDOffset')`'               >> $submit
  echo ' '                                                  >> $submit
  #echo 'export SCRAM_ARCH=slc5_amd64_gcc434'                >> $submit
  echo 'export SCRAM_ARCH=slc5_amd64_gcc462'                >> $submit
  if [ "$Site" == "fnal" ] ; then
    echo 'source /uscmst1/prod/sw/cms/shrc uaf'             >> $submit
  fi 
  echo 'scramv1 project CMSSW CMSSW_'$Release               >> $submit
  echo 'cd CMSSW_'$Release'/src'                            >> $submit 
  echo 'eval `scramv1 runtime -sh`'                         >> $submit
  if [ $tarball -gt 0 ] ; then
    #echo 'cvs co -r V00-07-08 GeneratorInterface/LHEInterface ' >> $submit
    echo 'cvs co -r V00-07-10 GeneratorInterface/LHEInterface/data ' >> $submit
  fi
  echo 'scramv1 b'                                          >> $submit
  echo 'cd -'                                               >> $submit 
  echo ' '                                                  >> $submit
  if [ "$Site" == "fnal" ] ; then
    echo "cp $pyCfg"' temp_${INPUT}.py'                     >> $submit
  elif   [ "$Site" == "cern" ] ; then
    echo "cp $WFWorkArea$pyCfg"' temp_${INPUT}.py'          >> $submit
  fi
  echo 'sed -ie  s/1111111/${SEED}/ temp_${INPUT}.py'       >> $submit
  echo 'cmsRun temp_${INPUT}.py &> '$Dataset'_${INPUT}.log' >> $submit 
  echo ' '                                                  >> $submit
  echo 'ls -l'                                              >> $submit 
  echo ' '                                                  >> $submit
  echo 'tar czf '$Dataset'_${INPUT}.log.tgz '$Dataset'_${INPUT}.log' >> $submit       
  if [ "$Site" == "fnal" ] ; then
    echo 'xrdcp -d 2 output.lhe '$eosDir'/'$Dataset'_${SEED}.lhe' >> $submit
    echo 'if [ $? -ne 0 ] ; then'                           >> $submit
    echo '  sleep 5m'                                       >> $submit
    echo '  xrdcp -d 2 output.lhe '$eosDir'/'$Dataset'_${SEED}.lhe' >> $submit
    echo '  if [ $? -ne 0 ] ; then'                         >> $submit
    echo '    sleep 15m'                                    >> $submit
    echo '    xrdcp -d 2 output.lhe '$eosDir'/'$Dataset'_${SEED}.lhe' >> $submit
    echo '  fi'                                             >> $submit
    echo 'fi'                                               >> $submit    
  elif [ "$Site" == "cern" ] ; then
    echo 'scp -o StrictHostKeyChecking=no '$Dataset'_${INPUT}.log.tgz '$subHOST':'$LogDir'/.'  >> $submit
    echo 'xrdcp -np output.lhe '$eosDir'/'$Dataset'_${SEED}.lhe' >> $submit 
  fi
  chmod +x $submit

  if [ "$Site" == "fnal" ] ; then
    # FNAL: Need to create EosDir 
    #ssh $fnaluser@cmslpc-sl5 whoami
    #ssh $fnaluser@cmslpc-sl5 mkdir /eos/uscms/store/lhe/$eosnum

    # FNAL: Need a jdl file

    jdl=$WFWorkArea$requestID'.jdl'
    cp /dev/null $jdl 
    echo 'universe = vanilla'                               >> $jdl    
    echo '+DESIRED_Archs="INTEL,X86_64"'                    >> $jdl
    echo '+DESIRED_Sites = "T1_US_FNAL"'                    >> $jdl
    echo 'Requirements = stringListMember(GLIDEIN_CMSSite,DESIRED_Sites)&& stringListMember(Arch, DESIRED_Archs)'  >> $jdl
    echo 'Executable = '$submit                             >> $jdl
    echo 'should_transfer_files = YES'                      >> $jdl
    echo 'when_to_transfer_output = ON_EXIT'                >> $jdl
    echo 'transfer_input_files = '$WFWorkArea$pyCfg         >> $jdl
    echo 'transfer_output_files = '$Dataset'_$(process).log.tgz' >> $jdl
    echo 'transfer_output_remaps = "'$Dataset'_$(process).log.tgz = '$LogDir'/'$Dataset'_$(process).log.tgz"' >> $jdl
    echo 'stream_error = false'                             >> $jdl
    echo 'stream_output = false'                            >> $jdl
    echo 'Output = ' $WFWorkArea$Dataset'_$(cluster)_$(process).out'  >> $jdl
    echo 'Error  = ' $WFWorkArea$Dataset'_$(cluster)_$(process).err'  >> $jdl
    echo 'Log    = ' $WFWorkArea$Dataset'_$(cluster)_$(process).log'  >> $jdl
    echo 'notification = NEVER'                             >> $jdl
    echo 'Arguments = $(process)'                           >> $jdl
    echo 'priority = 10'                                    >> $jdl
    echo 'Queue '$nJobs                                     >> $jdl

    res=`(condor_submit $jdl)`
    echo $res
    taskID=`(echo $res | awk -F'submitted to cluster' '{print $2}' | awk -F'.' '{print $1}' | sed 's/ //g' )` 
    joblist=$BaseDir'/'$requestID'.'$taskID'.joblist'
    echo $joblist
    cp /dev/null $joblist
    for ((i=0 ; i < $nJobs ; ++ i )) ; do
      echo $taskID'.'$i' '$i >> $joblist 
    done

  elif   [ "$Site" == "cern" ] ; then
    taskID=`(mktemp -p $PWD -t .XXX | awk -F'.' '{print $2}')`
    for (( iJob=$iJobStart ; iJob<=$nJobs ; ++iJob )) ; do  
      echo bsub -u $email -q $queue -o $WFWorkArea$Dataset'_'$taskID'_'$iJob.out -J $taskID'_'$iJob $submit $iJob
           $BSUB -u $email -q $queue -o $WFWorkArea$Dataset"_"$taskID"_"$iJob.out -J $taskID"_"$iJob $submit $iJob
    done 
  else
    echo '[LHEProd::Submit] ERROR Unknown Site:' $Site
    exit
  fi
  cp /dev/null $lockFile
  cp /dev/null $actiFile
  echo $dir $lhein $taskID $nJobs $Site $SEEDOffset >> $actiFile
} 

# ------------------------ EXTERNAL WF REGISTRATION -----------------------------
extsub_lhe()
{

  chk_afs
  lhe=$lhein
  parse_config

  if [ "$Site" != "cern" ] ; then
    echo '[LHEProd::Extsub] ERROR: External WF only possible at CERN master node'
    exit
  fi

  if [ "$extsub" == "fnal" ] ; then
    PWD=`pwd`
    BaseDir=`pwd`'/'$dir
    lockFile=$BaseDir'/'$requestID'.lock'
    actiFile=$BaseDir'/'$requestID'.active'
    echo -en "[LHEProd::Extsub] INFO : Do you want to register this WorkFlow at $extsub ? [y/n] "
    read a
    case $a in
      y) echo "... Registering External WF ..." ;;
      *) exit ;;
    esac
    LogDir=$BaseDir'/LogFiles_'$requestID'/'
    mkdir -p $LogDir
    WFWorkArea=$WorkArea$requestID'/'
    mkdir -p $WFWorkArea
    cp /dev/null $lockFile
    cp /dev/null $actiFile
    taskID=`(mktemp -p $PWD -t .XXX | awk -F'.' '{print $2}')`
    echo $dir $lhein $taskID $nJobs $extsub >> $actiFile

  else
    echo '[LHEProd::Extsub] ERROR: Unknown External Site : '$extsub
  fi
}

# ------------------------  WORFLOW(S) STATUS ------------------------------------
sta_lhe() 
{


  echo  
  echo "--------------------------------------------------------------------"
  echo '   Last Update    : '`date`
  echo '   Worknode       : '`hostname`
  diskA=`(df -h . | grep -v Use | awk '{print $4}')`    
  diskU=`(df -h . | grep -v Use | awk '{print $5}')`    
  echo '   Disk Free Space: '$diskA "($diskU Use)   "
  load=`(uptime | awk -F"load average:" '{print $2}' )`
  echo '   Load average   :'$load  
  echo "--------------------------------------------------------------------"
  if [ "$Site" == "cern" ] ; then
    nRunTot=`($BJOBS  | grep "RUN"  | wc | awk '{print $1}')`
    nRunQDet=`($BJOBS  | grep "RUN"  | awk '{print $4}' | uniq -c | awk '{print $2":"$1}' )`
    nPendTot=`($BJOBS | grep "PEND" | wc | awk '{print $1}')`
    nPendQDet=`($BJOBS  | grep "PEND"  | awk '{print $4}' | uniq -c | awk '{print $2":"$1}' )`
    echo '   # Runing  Jobs : '$nRunTot ' ['$nRunQDet']'
    echo '   # Pending Jobs : '$nPendTot ' ['$nPendQDet']'
    nSync=`($BJOBS  | grep "Sync" | wc | awk '{print $1}')`
    nSyncR=`($BJOBS  | grep "Sync" | grep "RUN" | wc | awk '{print $1}')`
    nSyncP=`($BJOBS  | grep "Sync" | grep "PEND" | wc | awk '{print $1}')`
    echo '   # Sync    Jobs : '$nSync ' [ Running : '$nSyncR' / Pending : '$nSyncP' ]'
  elif [ "$Site" == "fnal" ] ; then
    nRunTot=`(condor_q | grep "cmsdataops" | awk '{print $6}' | grep "R" | wc | awk '{print $1}')`
    nPendTot=`(condor_q | grep "cmsdataops" | awk '{print $6}' | grep "I" | wc | awk '{print $1}')`
    echo '   # Runing  Jobs : '$nRunTot 
    echo '   # Pending Jobs : '$nPendTot 
  else
    exit
  fi
  echo "--------------------------------------------------------------------"
  echo


  lheact=$lhein 
  activeLHE=`(find . | grep ".active")`
  for iLHE in $activeLHE ; do
    dir=`(cat $iLHE | awk '{print $1}')`
    lhe=`(cat $iLHE | awk '{print $2}')`
    if [ "$lheact" != "NULL" ] ; then
      if [ "$lhe" != "$lheact" ] ; then
        continue
      fi
    fi
    lhein=$lhe
    taskID=`(cat $iLHE | awk '{print $3}' | sed 's\:\ \g' )`
    nSubmit=`(cat $iLHE | awk '{print $4}')`    
    runSite=`(cat $iLHE | awk '{print $5}')`
    FirstSeedOffSet=`(cat $iLHE | awk '{print $6}')`
    if [ -n "$FirstSeedOffSet" ]; then
      SEEDOffset=$FirstSeedOffSet
    fi
    lJobs=""
    nRun=0
    nPend=0 
    if [ "$Site" == "cern" ] && [ "$runSite" == "cern" ] ; then
      lJobs=`($BJOBS | grep $taskID'_' | grep "RUN" | awk '{print $7}' | awk -F "_" '{print $2}')`
      lJobs=$lJobs' '`($BJOBS | grep $taskID'_' | grep "PEND" | awk '{print $6}' | awk -F "_" '{print $2}')`
      nRun=`($BJOBS  | grep $taskID'_' | grep "RUN"  | wc | awk '{print $1}')`
      nPend=`($BJOBS | grep $taskID'_' | grep "PEND" | wc | awk '{print $1}')`
    elif [ "$Site" == "fnal" ] && [ "$runSite" == "fnal" ] ; then
      for itaskID in $taskID ; do
        lJobsTmp=`(condor_q | grep $itaskID'.' | awk '{print $1}' )`
        nRunTmp=`(condor_q  | grep $itaskID'.' | awk '{print $6}' | grep "R" | wc | awk '{print $1}')`
        nPendTmp=`(condor_q | grep $itaskID'.' | awk '{print $6}' | grep "I" | wc | awk '{print $1}')`
        joblist=$dir'/'$lhein'.'$itaskID'.joblist'
        for ilJobsTmp in $lJobsTmp  ; do
          lJobs=$lJobs' '`(grep $ilJobsTmp $joblist | awk '{print $2}')`    
        done
        nRun=`(expr $nRun + $nRunTmp)`
        nPend=`(expr $nPend + $nPendTmp)`
      done
    fi
    parse_config 
    BaseDir=`pwd`'/'$dir
    BadSeeds=$BaseDir'/'$requestID'.badseeds'
    if [ -f $BadSeeds ] ; then
      nBadSeeds=`(cat $BadSeeds | wc | awk '{print $1}')`
    else
      nBadSeeds=0
    fi
    if [ "$Site" == "cern" ] ; then
      lFiles=`(xrd eoscms dirlist /eos/cms/store/lhe/$eosnum | grep $Dataset | grep eos | awk '{print $5}' | awk -F"/" '{print $NF}')`
      nFiles=`(xrd eoscms dirlist /eos/cms/store/lhe/$eosnum | grep $Dataset | grep eos | awk '{print $5}' | wc | awk '{print $1}' )`
    elif [ "$Site" == "fnal" ] ; then
      lFiles=`(ssh $fnaluser@cmslpc-sl5 ls /eos/uscms/store/lhe/$eosnum 2> /dev/null)`
      nFiles=`(echo $lFiles | wc | awk '{print $2}' )`
    fi 
    nFailed=$(($nSubmit - $nRun - $nPend - $nFiles -$nBadSeeds )) 
    if [ $nFailed -lt 0 ] ; then
      nFailed=0
    fi

    if [ "$Site" == "$runSite" ] ; then
      echo '  --> Submitted : '$nSubmit' [ Running : '$nRun' / Pending : '$nPend' ]' 
      echo '  --> Finished  : '$nFiles   
      echo '  --> Bad Seeds : '$nBadSeeds
      echo '  --> Failed    : '$nFailed
    else
      echo '  --> WF running at : '$runSite
      echo '  --> Expected Files: '$nJobs
      echo '  --> Synced   Files: '$nFiles 
    fi
    echo

    if [ $FindFailedJob -eq 1 ] ; then
     echo '  --> Getting list of failed Jobs (be patient ....)' 
     if [ $nFailed -gt 0 ] ; then
       lFailed=""
       lFailedJobs=""
       lFailSeeds=""

       if [ "$Site" == "cern" ] ; then
         iStart=1
         iStop=$nSubmit
       elif [ "$Site" == "fnal" ] ; then
         iStart=0
         iStop=`(expr $nSubmit - 1)`
       else
         exit
       fi

       expjoblist=`(mktemp)`
       for (( iJob=$iStart ; iJob<=$iStop ; ++iJob )) ; do
         echo $iJob >> $expjoblist
       done 
       expjoblists=`(mktemp)`
       sort -n $expjoblist >> $expjoblists       

       subjoblist=`(mktemp)`
       for jRP in $lJobs ; do
         echo $jRP >> $subjoblist
       done 
       subjoblists=`(mktemp)`
       sort -n $subjoblist >> $subjoblists

       filjoblist=`(mktemp)`    
       for iFile in $lFiles ; do  
         SEED=`(echo $iFile | awk -F'_' '{print $NF}' | awk -F'.' '{print $1}')`
         iJob=`(expr $SEED - $SEEDOffset)`
         echo $iJob >> $filjoblist     
       done 
       filjoblists=`(mktemp)`
       sort -n $filjoblist >> $filjoblists

       difjoblist=`(mktemp)`
       diff $expjoblists $subjoblists | grep "<" | awk '{print $2}' > $difjoblist
       badjoblist=`(mktemp)`
       diff $difjoblist $filjoblists | grep "<" | awk '{print $2}' > $badjoblist  

       if [ -f $BadSeeds ] ; then
         BadSeedJob=`(mktemp)`
         BadSeedJobs=`(mktemp)`
         for iSeed in `(cat $BadSeeds)` ; do
           iJob=`(expr $iSeed - $SEEDOffset)`
           echo $iJob >> $BadSeedJob 
         done
         sort -n $BadSeedJob >> $BadSeedJobs
         lFailed=`(diff $badjoblist $BadSeedJobs | grep "<" | awk '{print $2}')`
#        diff $badjoblist $BadSeedJobs
         rm $BadSeedJob
         rm $BadSeedJobs
       else
         lFailed=`(diff $difjoblist $filjoblists | grep "<" | awk '{print $2}')`
       fi

       rm $badjoblist
       rm $difjoblist
       rm $expjoblist  
       rm $subjoblist
       rm $filjoblist
       rm $expjoblists 
       rm $subjoblists
       rm $filjoblists


#       for (( iJob=$iStart ; iJob<=$iStop ; ++iJob )) ; do 
#         bJobRP=0 
#         for jRP in $lJobs ; do      
#           if [ "$iJob" == "$jRP" ] ; then
#             bJobRP=1
#           fi
#         done
#         if [ $bJobRP -eq 0 ] ; then
#           SEED=`(expr $iJob + $SEEDOffset)`
#           expFile=$Dataset'_'$SEED'.lhe'
#           bJobF=0
#           for iFile in $lFiles ; do
#             if [ "$iFile" == "$expFile" ] ; then
#               bJobF=1
#             fi
#           done
#           if [ $bJobF -eq 0 ] ; then
#             lFailed=$lFailed' '$iJob
#             lFailSeeds=$lFailSeeds' '$SEED
#           fi 
#         fi
#       done


       BaseDir=`pwd`'/'$dir
       LogDir=$BaseDir'/LogFiles_'$requestID'/'

       for iJob in $lFailed ; do
         logFile=$LogDir'/'$Dataset'_'$iJob'.log.tgz'
         tar xzfO $logFile 2> /dev/null | grep  "%MSG-MG5 Error: The are less events" &> /dev/null
         if [ $? -eq 0 ] ; then
           SEED=`(expr $iJob + $SEEDOffset)`
           lFailSeeds=$lFailSeeds' '$SEED
         else   
           lFailedJobs=$lFailedJobs' '$iJob
         fi
       done 

       echo '  --> Failed Job(s) : ' $lFailedJobs 
       echo '  --> Failed Seed(s): ' $lFailSeeds
       echo
     fi
    fi   

  done
}


# ------------------------ Check #evt ---------------------------------------------
check_nevt()
{
 
  chk_afs
  if [ "$lhein" == "NULL" ] ; then
    echo "[LHEProd::CheckNevt] ERROR: <lheID> not specified "
    exit
  fi

  lheact=$lhein
  activeLHE=`(find . | grep ".active")`
  for iLHE in $activeLHE ; do
    dir=`(cat $iLHE | awk '{print $1}')`
    lhe=`(cat $iLHE | awk '{print $2}')`
    taskID=`(cat $iLHE | awk '{print $3}')`
    if [ "$lheact" != "NULL" ] ; then
      if [ "$lhe" != "$lheact" ] ; then
        continue 
      fi
    fi
    lhein=$lhe
    parse_config 

    BaseDir=`pwd`'/'$dir
    LogDir=$BaseDir'/LogFiles_'$requestID'/'
    mkdir -p $LogDir
    WFWorkArea=$WorkArea$requestID'/'
    mkdir -p $WFWorkArea
    submit=$WFWorkArea$requestID'.checknevt.sub'
    cp /dev/null $submit
    chmod +x $submit 
    subHOST=`hostname`

    echo '#!/bin/sh'                                          >> $submit
    echo ' '                                                  >> $submit
    echo 'lFiles=`(xrd eoscms dirlist /eos/cms/store/lhe/'$eosnum' | grep '$Dataset' | grep eos | awk '\''{print $5}'\'')`'  >> $submit
    echo ' '                                                  >> $submit
    echo 'logFile='$requestID'.checknevt.log'                 >> $submit
    echo 'cp /dev/null $logFile'                              >> $submit
    echo 'echo " " >> $logFile '                              >> $submit
    echo 'echo "Checking #Evt/File for WF: '$requestID' ('$Dataset')" >> $logFile' >> $submit 
    echo 'echo " " >> $logFile '                                  >> $submit
    echo 'nEvtTot=0'                                          >> $submit  
    echo 'for iFile in $lFiles ; do'                          >> $submit
    echo '  nEvtFile=`(xrd eoscms cat $iFile | grep "<event>" | wc | awk '\''{print $1}'\'')`' >> $submit
    echo '  nEvtTot=`(expr $nEvtTot + $nEvtFile)`'            >> $submit
    echo '  if [ "'$EvtJob'" != "$nEvtFile" ] ; then'         >> $submit
    echo '    echo "$iFile --> Missing Events ( #evt/File : $nEvtFile / '$EvtJob' )" >> $logFile' >> $submit
    echo '  fi'                                               >> $submit 
    echo 'done'                                               >> $submit  
    echo 'echo " " >> $logFile '                                  >> $submit
    echo 'echo " ---> Total #Events Produced = $nEvtTot / '$Events'">> $logFile'    >> $submit
    echo 'if [ $nEvtTot -lt '$Events' ] ; then'               >> $submit
    echo '  echo " ---> MISSING EVENTS !!!!" >> $logFile'     >> $submit
    echo 'fi'                                                 >> $submit 
    echo ' '                                                  >> $submit
    echo 'mail '$email',arnaud.pin@cern.ch -s '$requestID'_Check_NumEvt < $logFile'    >> $submit 
    echo 'scp -o StrictHostKeyChecking=no $logFile '$subHOST':'$LogDir  >> $submit
   
    echo '... Running file check in bkgd ... you will receive an email ...' 
 
    #echo $submit
    $BSUB -sp 70 -u $email -q $queue -o $WFWorkArea$Dataset"_"$taskID"_"chkevt.out -J ChkEv$taskID $submit 
    #nohup $submit &> /dev/null &

  done
}

# ------------------------ RESUBMIT FAILED JOBS -----------------------------------

resub_lhe()
{

  chk_afs
  if [ "$lhein" == "NULL" ] ; then
    echo "[LHEProd::Resub] ERROR: <lheID> not specified "
    exit
  fi

  lheact=$lhein
  activeLHErsb=`(find . | grep ".active")`
  for iLHErsb in $activeLHErsb ; do
    dir=`(cat $iLHErsb | awk '{print $1}')`
    lhe=`(cat $iLHErsb | awk '{print $2}')`
    runSite=`(cat $iLHErsb | awk '{print $5}')`
    if [ "$Site" != "$runSite" ] ; then
      continue
    fi
    OldtaskID=`(cat $iLHErsb | awk '{print $3}')`
    if [ "$lheact" != "NULL" ] ; then
      if [ "$lhe" != "$lheact" ] ; then
        continue
      fi
    fi
    lhein=$lhe
    FindFailedJob=1
    sta_lhe 
    dir=`(cat $iLHErsb | awk '{print $1}')`
    nJobs=`(cat $iLHErsb | awk '{print $4}')`
    echo $iLHErsb $lhein $dir $nJobs
    echo -en "[LHEProd::Inject] INFO : Do you want to re-submit this WorkFlow ? [y/n] "
    read a
    if [ "$a" == "y" ] ; then
      BaseDir=`pwd`'/'$dir
      LogDir=$BaseDir'/LogFiles_'$requestID'/'
      WFWorkArea=$WorkArea$requestID'/'
      submit=$WFWorkArea$requestID'.sub'
      if [ "$Site" == "cern" ] ; then
        for iJob in $lFailedJobs ; do
          echo bsub -sp 60 -u $email -q $queue -o $WFWorkArea$Dataset'_'$taskID'_'$iJob.resub.out -J $taskID'_'$iJob $submit $iJob
               $BSUB -sp 60 -u $email -q $queue -o $WFWorkArea$Dataset'_'$taskID'_'$iJob.resub.out -J $taskID'_'$iJob $submit $iJob
        done

        
        if [ `(echo $lFailSeeds | wc | awk '{print $1}')` -gt 0 ] ; then
          BadSeeds=$BaseDir'/'$requestID'.badseeds'
          touch $BadSeeds 
          for iSeed in $lFailSeeds ; do
            echo $iSeed >> $BadSeeds
            nJobs=`(expr $nJobs + 1)`
            iJob=$nJobs
            echo bsub -sp 60 -u $email -q $queue -o $WFWorkArea$Dataset'_'$taskID'_'$iJob.resub.out -J $taskID'_'$iJob $submit $iJob
                 $BSUB -sp 60 -u $email -q $queue -o $WFWorkArea$Dataset"_"$taskID"_"$iJob.resub.out -J $taskID"_"$iJob $submit $iJob
          done
          echo $dir $lhein $OldtaskID $nJobs $Site > $iLHErsb
        fi 
      elif [ "$Site" == "fnal" ] ; then 
        jdl=$WFWorkArea$requestID'.resub.jdl'
        cp /dev/null $jdl
        echo 'universe = vanilla'                               >> $jdl
        echo '+DESIRED_Archs="INTEL,X86_64"'                    >> $jdl
        echo '+DESIRED_Sites = "T1_US_FNAL"'                    >> $jdl
        echo 'Requirements = stringListMember(GLIDEIN_CMSSite,DESIRED_Sites)&& stringListMember(Arch, DESIRED_Archs)'  >> $jdl
        echo 'Executable = '$submit                             >> $jdl
        echo 'should_transfer_files = YES'                      >> $jdl
        echo 'when_to_transfer_output = ON_EXIT'                >> $jdl
        echo 'transfer_input_files = '$WFWorkArea$pyCfg         >> $jdl
        echo 'stream_error = false'                             >> $jdl
        echo 'stream_output = false'                            >> $jdl
        echo 'notification = NEVER'                             >> $jdl
        echo 'priority = 15'                                    >> $jdl
        echo ' '                                                >> $jdl  
        for iJob in $lFailedJobs ; do
          echo 'transfer_output_files = '$Dataset'_'$iJob'.log.tgz' >> $jdl
          echo 'transfer_output_remaps = "'$Dataset'_'$iJob'.log.tgz = '$LogDir'/'$Dataset'_'$iJob'.log.tgz"' >> $jdl
          echo 'Output = ' $WFWorkArea$Dataset'_$(cluster)_'$iJob'.out'  >> $jdl
          echo 'Error  = ' $WFWorkArea$Dataset'_$(cluster)_'$iJob'.err'  >> $jdl
          echo 'Log    = ' $WFWorkArea$Dataset'_$(cluster)_'$iJob'.log'  >> $jdl
          echo 'Arguments = '$iJob                                >> $jdl 
          echo 'Queue '                                           >> $jdl
          echo ' '                                                >> $jdl  
        done
        if [ `(echo $lFailSeeds | wc | awk '{print $1}')` -gt 0 ] ; then
          BadSeeds=$BaseDir'/'$requestID'.badseeds'
          touch $BadSeeds
          for iSeed in $lFailSeeds ; do
            echo $iSeed >> $BadSeeds
            nJobs=`(expr $nJobs + 1)`
            iJob=`(expr $nJobs - 1)`
            echo 'transfer_output_files = '$Dataset'_'$iJob'.log.tgz' >> $jdl
            echo 'transfer_output_remaps = "'$Dataset'_'$iJob'.log.tgz = '$LogDir'/'$Dataset'_'$iJob'.log.tgz"' >> $jdl
            echo 'Output = ' $WFWorkArea$Dataset'_$(cluster)_'$iJob'.out'  >> $jdl
            echo 'Error  = ' $WFWorkArea$Dataset'_$(cluster)_'$iJob'.err'  >> $jdl
            echo 'Log    = ' $WFWorkArea$Dataset'_$(cluster)_'$iJob'.log'  >> $jdl
            echo 'Arguments = '$iJob                                >> $jdl
            echo 'Queue '                                           >> $jdl
            echo ' '                                                >> $jdl
          done
        fi 

        res=`(condor_submit $jdl)`
        echo $res
        NewtaskID=`(echo $res | awk -F'submitted to cluster' '{print $2}' | awk -F'.' '{print $1}' | sed 's: ::g' )`
        echo $dir $lhein $OldtaskID':'$NewtaskID $nJobs $Site > $iLHErsb
        joblist=$dir'/'$lhein'.'$NewtaskID'.joblist'
        cp /dev/null $joblist
        i=0   
        for iJob in $lFailed ; do
          echo $NewtaskID'.'$i' '$iJob >> $joblist 
          i=`(expr $i + 1)`
        done
      fi 
    fi
  done
}

# ------------------------ Kill LHE WF -------------------------------------
kill_lhe()
{

  if [ "$lhein" == "NULL" ] ; then
    echo "[LHEProd::Kill] ERROR: <lheID> not specified "
    exit
  fi

  lheact=$lhein
  activeLHE=`(find . | grep ".active")`
  for iLHEkil in $activeLHE ; do
    lhe=`(cat $iLHEkil | awk '{print $2}')`
    runSite=`(cat $iLHEkil | awk '{print $5}')`
    if [ "$Site" != "$runSite" ] ; then
      continue
    fi
    if [ "$lhe" != "$lheact" ] ; then
      continue
    fi
    lhein=$lhe 
    dir=`(cat $iLHEkil | awk '{print $1}')`
    parse_config
    sta_lhe
    taskID=`(cat $iLHEkil | awk '{print $3}' | sed 's\:\ \g')`

    echo -en "[LHEProd::Extsub] INFO : Do you want to KILL JOBS for this WorkFlow ? [y/n] "
    read a
    case $a in
      y) echo "... Killing all Jobs for $lhein ..." ;;
      *) exit ;;
    esac
    if [ "$Site" == "cern" ] ; then
      $BJOBS | grep $taskID'_' | awk '{print $1}' | xargs -n 1 bkill 
    elif [ "$Site" == "fnal" ] ; then
      for itaskID in $taskID ; do
        condor_rm $itaskID
      done       
    fi
  done


}

# ------------------------ Add jobs to LHE WF -------------------------------------

add_lhejob()
{
  chk_afs

  if [ "$lhein" == "NULL" ] ; then
    echo "[LHEProd::AddJob] ERROR: <lheID> not specified "
    exit
  fi 

  lheact=$lhein 
  activeLHE=`(find . | grep ".active")`
  for iLHEadd in $activeLHE ; do
    lhe=`(cat $iLHEadd | awk '{print $2}')`
    runSite=`(cat $iLHEadd | awk '{print $5}')`
    if [ "$Site" != "$runSite" ] ; then
      continue
    fi
    if [ "$lhe" != "$lheact" ] ; then
      continue
    fi
    echo $lhe
    lhein=$lhe
    dir=`(cat $iLHEadd | awk '{print $1}')`
    taskID=`(cat $iLHEadd | awk '{print $3}')`
    OldtaskID=`(cat $iLHEadd | awk '{print $3}')`
    parse_config
    dir=`(cat $iLHEadd | awk '{print $1}')`
    nJobs=`(cat $iLHEadd | awk '{print $4}')`
    WFWorkArea=$WorkArea$requestID'/'
    submit=$WFWorkArea$requestID'.sub'

    if [ "$Site" == "cern" ] ; then
      # New Start / Stop range
      iJobStart=`(expr $nJobs + 1)`
      nJobs=`(expr $nJobs + $addjob )`
      for (( iJob=$iJobStart ; iJob<=$nJobs ; ++iJob )) ; do  
        echo bsub -u $email -q $queue -o $WFWorkArea$Dataset'_'$taskID'_'$iJob.out -J $taskID'_'$iJob $submit $iJob
             $BSUB -u $email -q $queue -o $WFWorkArea$Dataset"_"$taskID"_"$iJob.out -J $taskID"_"$iJob $submit $iJob
      done 
      echo $dir $lhe $taskID $nJobs $Site > $iLHEadd
    elif [ "$Site" == "fnal" ] ; then
      # New Start / Stop range
      iJobStart=$nJobs
      nJobs=`(expr $nJobs + $addjob )`
      # FNAL jdl
      jdl=$WFWorkArea$requestID'.resub.jdl'
      cp /dev/null $jdl
      echo 'universe = vanilla'                               >> $jdl
      echo '+DESIRED_Archs="INTEL,X86_64"'                    >> $jdl
      echo '+DESIRED_Sites = "T1_US_FNAL"'                    >> $jdl
      echo 'Requirements = stringListMember(GLIDEIN_CMSSite,DESIRED_Sites)&& stringListMember(Arch, DESIRED_Archs)'  >> $jdl
      echo 'Executable = '$submit                             >> $jdl
      echo 'should_transfer_files = YES'                      >> $jdl
      echo 'when_to_transfer_output = ON_EXIT'                >> $jdl
      echo 'transfer_input_files = '$WFWorkArea$pyCfg         >> $jdl
      echo 'stream_error = false'                             >> $jdl
      echo 'stream_output = false'                            >> $jdl
      echo 'notification = NEVER'                             >> $jdl
      echo 'priority = 10'                                    >> $jdl
      echo ' '                                                >> $jdl
      for (( iJob=$iJobStart ; iJob<$nJobs ; ++iJob )) ; do 
        echo 'transfer_output_files = '$Dataset'_'$iJob'.log.tgz' >> $jdl
        echo 'transfer_output_remaps = "'$Dataset'_'$iJob'.log.tgz = '$LogDir'/'$Dataset'_'$iJob'.log.tgz"' >> $jdl
        echo 'Output = ' $WFWorkArea$Dataset'_$(cluster)_'$iJob'.out'  >> $jdl
        echo 'Error  = ' $WFWorkArea$Dataset'_$(cluster)_'$iJob'.err'  >> $jdl
        echo 'Log    = ' $WFWorkArea$Dataset'_$(cluster)_'$iJob'.log'  >> $jdl
        echo 'Arguments = '$iJob                                >> $jdl
        echo 'Queue '                                           >> $jdl
        echo ' '                                                >> $jdl
      done 
      res=`(condor_submit $jdl)`
      echo $res
      NewtaskID=`(echo $res | awk -F'submitted to cluster' '{print $2}' | awk -F'.' '{print $1}' | sed 's: ::g' )`
      echo $dir $lhein $OldtaskID':'$NewtaskID $nJobs $Site > $iLHEadd
      joblist=$dir'/'$lhein'.'$NewtaskID'.joblist'
      cp /dev/null $joblist
      i=0
      for (( iJob=$iJobStart ; iJob<$nJobs ; ++iJob )) ; do
        echo $NewtaskID'.'$i' '$iJob >> $joblist
        i=`(expr $i + 1)`
      done  
    fi

  done
}

# ------------------------ Move Logs Out of afs -----------------------------------
clean_afs_log()
{
  lheact=$lhein
  activeLHE=`(find . | grep ".active")`
  for iLHE in $activeLHE ; do
    dir=`(cat $iLHE | awk '{print $1}')`
    lhe=`(cat $iLHE | awk '{print $2}')`
    if [ "$lheact" != "NULL" ] ; then
      if [ "$lhe" != "$lheact" ] ; then
        continue
      fi
    fi
    lhein=$lhe
    parse_config 
    WFWorkArea=$WorkArea$requestID'/'
    BaseDir=`pwd`'/'$dir
    LogDir=$BaseDir'/LogFiles_'$requestID'/'
    mkdir -p $LogDir
    mv $WFWorkArea/*.out $LogDir
  done
  if [ "$Site" == "cern" ] ; then
    mv $HOME/LSFJOB_* $LsfOutDir
  fi
}


# ------------------------ SYNC EXTERNAL WF ---------------------------------------
sync_lhe()
{

  chk_afs
  if [ "$Site" != "cern" ] ; then
    echo '[LHEProd::Sync] ERROR: External WF only possible at CERN master node'
    exit
  fi

  if [ "$lhein" == "NULL" ] ; then
    echo "[LHEProd::Sync] ERROR: <lheID> not specified "
    exit
  fi 

  lheact=$lhein
  activeLHE=`(find . | grep ".active")`
  for iLHEsnc in $activeLHE ; do 
    dir=`(cat $iLHEsnc | awk '{print $1}')`
    lhe=`(cat $iLHEsnc | awk '{print $2}')`
    runSite=`(cat $iLHEsnc | awk '{print $5}')`
    if [ "$Site" == "$runSite" ] ; then
      continue
    fi
    if [ "$lheact" != "NULL" ] ; then
      if [ "$lhe" != "$lheact" ] ; then
        continue
      fi
    fi
    lhein=$lhe
    sta_lhe
    echo -en "[LHEProd::Sync] INFO : Do you want to Sync this WorkFlow ? [y/n] "
    read a
    if [ "$a" == "y" ] ; then
      dir=`(cat $iLHEsnc | awk '{print $1}')`
      taskID=`(cat $iLHEsnc | awk '{print $3}' | sed 's\:\ \g' )` 
      BaseDir=`pwd`'/'$dir
      LogDir=$BaseDir'/LogFiles_'$requestID'/'
      WFWorkArea=$WorkArea$requestID'/'
      if [ "$runSite" == "fnal" ] ; then
        lockFile=$WFWorkArea$requestID'.synclock'
        if [ -f $lockFile ] ; then
          echo '[LHEProd.sh::Submit] ERROR lockFile exist:' $lockFile
          exit
        fi

        voms-proxy-init -cert $globusDir/usercert.pem -key $globusDir/userkey.pem -valid 168:00 
        certfull=`(voms-proxy-info | grep path | awk -F":" '{print $2}' | sed 's: ::g')`
        certshort=`(echo $certfull | awk -F"/" '{print $NF}')`
        cp $certfull $WFWorkArea
 
        syncJob=$WFWorkArea$requestID'.sync'
        cp /dev/null $syncJob
        echo '#!/bin/sh'                                          >> $syncJob
        echo ' '                                                  >> $syncJob 
        echo 'PWD=`pwd`'                                          >> $syncJob  
        echo 'mv '$WFWorkArea$certshort' /tmp/'$certshort         >> $syncJob
        echo 'srmls '$fnalsrm$fnaleos'/'$eosnum' | grep '$Dataset '| awk '\''{print $2":"$1}'\' ' > rFiles '  >> $syncJob
        echo 'echo "Remote Files : " `(wc rFiles)` '              >> $syncJob
#       echo 'xrd eoscms dirlist /eos/cms/store/lhe/'$eosnum' | grep '$Dataset' | grep eos | awk '\''{print $5":"$2}'\'' > lFiles'  >> $syncJob
        echo 'for irFile in `(cat rFiles)` ; do'                  >> $syncJob
        echo '  rFile=`(echo $irFile | awk -F'\'':'\'' '\''{print $1}'\'' | awk -F'\''/'\'' '\''{print $NF}'\'')`'  >> $syncJob
        echo '  rSize=`(echo $irFile | awk -F'\'':'\'' '\''{print $2}'\'')`'  >> $syncJob 
        echo '  if [ $rSize -gt 0 ] ; then '                      >> $syncJob
        echo '    echo "Copying: "$rFile'                         >> $syncJob
        echo '    lcg-cp --nobdii -D srmv2 '$fnalsrm$fnaleos'/'$eosnum'/$rFile file:///$PWD/$rFile'      >> $syncJob 
        echo '    if [ $? -eq 0 ] ; then '                        >> $syncJob
        echo '      xrdcp -np $rFile '$eosDir'/$rFile'            >> $syncJob
        echo '    fi'                                             >> $syncJob 
        echo '    ls -l '                                         >> $syncJob
        echo '    rm $rFile'                                      >> $syncJob 
        echo '  else'                                             >> $syncJob 
        echo '    echo $rFile "has zero size !" '                 >> $syncJob 
        echo '  fi'                                               >> $syncJob 
        echo 'done'                                               >> $syncJob
        echo 'rm /tmp/'$certshort                                 >> $syncJob
        echo 'rm '$lockFile                                       >> $syncJob
        
        chmod +x $syncJob
        echo bsub -u $email -q 1nd -o $WFWorkArea$Dataset'_'sync.out -J Sync$taskID $syncJob
        $BSUB -sp 70 -u $email -q $queue -o $WFWorkArea$Dataset"_"sync.out -J Sync$taskID $syncJob
        touch $lockFile
      else
        echo '[LHEProd::Sync] ERROR: Unknown External Site : '$extsub
      fi 
    fi
  done


}

# ------------------------ CLOSE WORFLOW ------------------------------------------
close_lhe()
{
  if [ "$lhein" == "NULL" ] ; then
    echo '[LHEProd::Close] ERROR lhe not specified'
    exit
  fi

  # Find back jobs
  activeLHE=`(find . | grep ".active")`
  Found=0
  for iLHE in $activeLHE ; do
    dir=`(cat $iLHE | awk '{print $1}')`
    lhe=`(cat $iLHE | awk '{print $2}')`
    if [ "$lhe" == "$lhein" ] ; then
      Found=1
      actiFile=$iLHE
    fi
  done

  if [ $Found -eq 0 ] ; then
    echo '[LHEProd::Close] ERROR lhe not in active WF: '$lhein
    exit
  fi

  # Check Status
  sta_lhe

  echo -en "[LHEProd::Close] INFO : Do you want to close this WorkFlow ? [y/n] "
  read a
  case $a in
    y) echo "... Closing WF ..." ;;
    *) exit ;;
  esac 


  doneFile=`(echo $actiFile | sed "s:active:done:")`
  mv $actiFile $doneFile
  sleep 5
  clean_afs_log &> /dev/null
  WFWorkArea=$WorkArea$requestID'/'
  mv $WFWorkArea $dir'/WorkArea_'$requestID 

  echo 
  echo "Please Update PREP Status as well"
  echo

}

#----------------------------------------------------------------------------------
#------------------------ DO EVERYTHING NOW ---------------------------------------
#----------------------------------------------------------------------------------



# Get Options

tgz="NULL"
dir="NULL"
lhein="NULL"
sub=0
extsub="NULL"
resub=0
kil=0
addjob=0
sta=0
inj=0
clo=0
cleanlog=0
chknevt=0
sync=0

nJobMax=0
iJobStart=1
tarball=1

FindFailedJob=0

for arg in $* ; do
  case $arg in
    -tgz)    tgz=$2        ; shift ; shift ;;
    -dir)    dir=$2        ; shift ; shift ;;
    -lhe)    lhein=$2      ; shift ; shift ;;
    -inject) inj=1                 ; shift ;;
    -submit) sub=1                 ; shift ;;
    -tarball) tarball=1            ; shift ;;
    -queue)  queue=$2      ; shift ; shift ;;
    -extsub) extsub=$2     ; shift ; shift ;;
    -njmax)  nJobMax=$2    ; shift ; shift ;;
    -jstart) iJobStart=$2  ; shift ; shift ;;
    -resub)  resub=1               ; shift ;;
    -addjob) addjob=$2     ; shift ; shift ;;
    -kill)   kil=1                 ; shift ;;
    -status) sta=1                 ; shift ;;
    -fjlist) FindFailedJob=1       ; shift ;;
    -sync)   sync=1                ; shift ;;
    -chknevt) chknevt=1            ; shift ;;
    -cleanlog) cleanlog=1          ; shift ;;
    -close)  clo=1                 ; shift ;;
    -h)      print_help                    ;;
  esac
done

# Do the job

if [ $inj -eq 1 ] ; then
  inj_lhe 
  exit
fi

if [ $sub -eq 1 ] ; then
  lhe=$lhein
  parse_config
  sub_lhe
  exit
fi

if [ "$extsub" != "NULL" ] ; then
  extsub_lhe 
  exit
fi

if [ $resub -eq 1 ] ; then
  resub_lhe
  exit
fi

if [ $addjob -gt 0 ] ; then
  add_lhejob
  exit
fi

if [ $kil -eq 1 ] ; then
  kill_lhe
  exit
fi

if [ $sta -eq 1 ] ; then
  sta_lhe
  exit
fi

if [ $sync -eq 1 ] ; then
  sync_lhe
  exit
fi

if [ $chknevt -eq 1 ] ; then
  check_nevt
  exit
fi

if [ $cleanlog -eq 1 ] ; then
  clean_afs_log
  exit
fi

if [ $clo -eq 1 ] ; then
  close_lhe
  exit
fi

# --- Nothing ?
print_help


