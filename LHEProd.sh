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

  queue="cmst0"
  chkqueue='1nd'
  eosBase="root://eoscms//eos/cms/store/lhe/"
  LsfOutDir="LsfOutDir"
  mkdir -p $LsfOutDir
  WorkArea=$HOME"/scratch0/LHEProdWorkArea/"
  mkdir -p $WorkArea

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

  LHEProd.sh -submit -dir <WFDir> -lhe <lheID> : 
       Submit lheID WF already dowloaded to WFDir

  LHEProd.sh -resub  [-lhe <lheID>] 
       Resubmit Failed jobs for all LHE ongoing WF or lheID WF   

  LHEProd.sh -addjob <#job> -lhe <lheID>
       Submit <#job> extra jobs for <lheID> WF

  LHEProd.sh -status [-lhe <lheID>] [-fjlist]:
       Get Status of all LHE ongoing WF or of lheID WF
       -fjlist : Get List of failed jobs (be patient)

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
  echo 'pyCfg      : '$dir$pyCfg
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
    lhein=$iLHE
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
  if [ "$Site" == "fnal" ] ; then
    echo 'export SCRAM_ARCH=slc5_amd64_gcc434'              >> $submit
    echo 'source /uscmst1/prod/sw/cms/shrc uaf'             >> $submit
    echo 'scramv1 project CMSSW CMSSW_'$Release             >> $submit
    echo 'cd CMSSW_'$Release'/src'                          >> $submit 
    echo 'eval `scramv1 runtime -sh`'                       >> $submit
    echo 'cd -'                                             >> $submit 
  elif   [ "$Site" == "cern" ] ; then
    echo 'source $HOME/EVAL_SH64 '$Release                  >> $submit
  fi
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
    taskID=`(echo $res | awk -F'submitted to cluster' '{print $2}' | awk -F'.' '{print $1}')` 
    joblist=$dir'/'$lhe'.'$taskID'.joblist'
    cp /dev/null $joblist
    for ((i=0 ; i < $nJobs ; ++ i )) ; do
      echo $taskID'.'$i' '$i >> $joblist 
    done

  elif   [ "$Site" == "cern" ] ; then
    taskID=`(mktemp -p $PWD -t .XXX | awk -F'.' '{print $2}')`
    for (( iJob=$iJobStart ; iJob<=$nJobs ; ++iJob )) ; do  
      echo bsub -u $email -q $queue -o $WFWorkArea$Dataset'_'$taskID'_'$iJob.out -J $taskID'_'$iJob $submit $iJob
           bsub -u $email -q $queue -o $WFWorkArea$Dataset'_'$taskID'_'$iJob.out -J $taskID'_'$iJob $submit $iJob
    done 
  else
    echo '[LHEProd::Submit] ERROR Unknown Site:' $Site
    exit
  fi
  cp /dev/null $lockFile
  cp /dev/null $actiFile
  echo $dir $lhein $taskID $nJobs >> $actiFile
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
    nRunTot=`(bjobs  | grep "RUN"  | wc | awk '{print $1}')`
    nRunQDet=`(bjobs  | grep "RUN"  | awk '{print $4}' | uniq -c | awk '{print $2":"$1}' )`
    nPendTot=`(bjobs | grep "PEND" | wc | awk '{print $1}')`
    nPendQDet=`(bjobs  | grep "PEND"  | awk '{print $4}' | uniq -c | awk '{print $2":"$1}' )`
    echo '   # Runing  Jobs : '$nRunTot ' ['$nRunQDet']'
    echo '   # Pending Jobs : '$nPendTot ' ['$nPendQDet']'
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
    if [ "$Site" == "cern" ] ; then
      lJobs=`(bjobs | grep $taskID'_' | awk '{print $7}' | awk -F "_" '{print $2}')`
      nRun=`(bjobs  | grep $taskID'_' | grep "RUN"  | wc | awk '{print $1}')`
      nPend=`(bjobs | grep $taskID'_' | grep "PEND" | wc | awk '{print $1}')`
    elif [ "$Site" == "fnal" ] ; then
      lJobs=""
      nRun=0
      nPend=0 
      for itaskID in $taskID ; do
        lJobsTmp=`(condor_q | grep $itaskID'.' | awk '{print $1}' )`
        nRunTmp=`(condor_q  | grep $itaskID'.' | awk '{print $6}' | grep "R" | wc | awk '{print $1}')`
        nPendTmp=`(condor_q | grep $itaskID'.' | awk '{print $6}' | grep "I" | wc | awk '{print $1}')`
        joblist=$dir'/'$lhe'.'$itaskID'.joblist'
        for ilJobsTmp in $lJobsTmp  ; do
          lJobs=$lJobs' '`(grep $ilJobsTmp $joblist | awk '{print $2}')`    
        done
        nRun=`(expr $nRun + $nRunTmp)`
        nPend=`(expr $nPend + $nPendTmp)`
      done
    fi
    parse_config 
    if [ "$Site" == "cern" ] ; then
      lFiles=`(xrd eoscms dirlist /eos/cms/store/lhe/$eosnum | grep $Dataset | grep eos | awk '{print $5}' | awk -F"/" '{print $NF}')`
      nFiles=`(xrd eoscms dirlist /eos/cms/store/lhe/$eosnum | grep $Dataset | grep eos | awk '{print $5}' | wc | awk '{print $1}' )`
    else
      lFiles=`(ssh $fnaluser@cmslpc-sl5 ls /eos/uscms/store/lhe/$eosnum)`
      nFiles=`(echo $lFiles | wc | awk '{print $2}' )`
    fi 
    nFailed=$(($nSubmit - $nRun - $nPend - $nFiles)) 
    if [ $nFailed -lt 0 ] ; then
      nFailed=0
    fi

    echo '  --> Submitted : '$nSubmit' [ Running : '$nRun' / Pending : '$nPend' ]' 
    echo '  --> Finished  : '$nFiles   
    echo '  --> Failed    : '$nFailed 
    echo

    if [ $FindFailedJob -eq 1 ] ; then
     echo '  --> Getting list of failed Jobs (be patient ....)' 
     if [ $nFailed -gt 0 ] ; then
       lFailed=""
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
       subjoblist=`(mktemp)`
       for jRP in $lJobs ; do
         echo $jRP >> $subjoblist
       done 
       filjoblist=`(mktemp)`    
       for iFile in $lFiles ; do  
         SEED=`(echo $iFile | awk -F'_' '{print $NF}' | awk -F'.' '{print $1}')`
         iJob=`(expr $SEED - $SEEDOffset)`
         echo $iJob >> $filjoblist     
       done 

       difjoblist=`(mktemp)`
       diff $expjoblist $subjoblist | grep "<" | awk '{print $2}' > $difjoblist
       lFailed=`(diff $difjoblist $filjoblist | grep "<" | awk '{print $2}')`

       rm $difjoblist
       rm $expjoblist  
       rm $subjoblist
       rm $filjoblist

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

       echo '  --> Failed Job(s) : ' $lFailed
       for iJob in $lFailed ; do
         SEED=`(expr $iJob + $SEEDOffset)`
         lFailSeeds=$lFailSeeds' '$SEED 
       done
       echo '  --> Failed Seed(s): ' $lFailSeeds
       echo
     fi
    fi   

  done
}


# ------------------------ Check #evt ---------------------------------------------
check_nevt()
{

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
    echo 'mail '$email' -s '$requestID'_Check_NumEvt < $logFile'    >> $submit 
    echo 'scp -o StrictHostKeyChecking=no $logFile '$subHOST':'$LogDir  >> $submit
   
    echo '... Running file check in bkgd ... you will receive an email ...' 
 
    #echo $submit
    #bsub -u $email -q $chkqueue -o $WFWorkArea$Dataset'_'$taskID'_'chkevt.out -J ChkEv$taskID $submit 
    nohup $submit &> /dev/null &

  done
}

# ------------------------ RESUBMIT FAILED JOBS -----------------------------------

resub_lhe()
{
  lheact=$lhein
  activeLHErsb=`(find . | grep ".active")`
  for iLHErsb in $activeLHErsb ; do
    dir=`(cat $iLHErsb | awk '{print $1}')`
    lhe=`(cat $iLHErsb | awk '{print $2}')`
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
        for iJob in $lFailed ; do
          echo bsub -u $email -q $queue -o $WFWorkArea$Dataset'_'$taskID'_'$iJob.resub.out -J $taskID'_'$iJob $submit $iJob
               bsub -u $email -q $queue -o $WFWorkArea$Dataset'_'$taskID'_'$iJob.resub.out -J $taskID'_'$iJob $submit $iJob
        done
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
        echo 'priority = 10'                                    >> $jdl
        echo ' '                                                >> $jdl  
        for iJob in $lFailed ; do
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
        echo $dir $lhein $OldtaskID':'$NewtaskID $nJobs > $iLHErsb
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

# ------------------------ Add jobs to LHE WF -------------------------------------

add_lhejob()
{

  if [ "$lhein" == "NULL" ] ; then
    echo "[LHEProd::AddJob] ERROR: <lheID> not specified "
    exit
  fi 

  lheact=$lhein 
  activeLHE=`(find . | grep ".active")`
  for iLHE in $activeLHE ; do
    lhe=`(cat $iLHE | awk '{print $2}')`
    if [ "$lhe" != "$lheact" ] ; then
      continue
    fi
    lhein=$lhe
    dir=`(cat $iLHE | awk '{print $1}')`
    taskID=`(cat $iLHE | awk '{print $3}')`
    parse_config
    nJobs=`(cat $iLHE | awk '{print $4}')`
    WFWorkArea=$WorkArea$requestID'/'
    submit=$WFWorkArea$requestID'.sub'

    if [ "$Site" == "cern" ] ; then
      # New Start / Stop range
      iJobStart=`(expr $nJobs + 1)`
      nJobs=`(expr $nJobs + $addjob )`
      for (( iJob=$iJobStart ; iJob<=$nJobs ; ++iJob )) ; do  
        echo bsub -u $email -q $queue -o $WFWorkArea$Dataset'_'$taskID'_'$iJob.out -J $taskID'_'$iJob $submit $iJob
             bsub -u $email -q $queue -o $WFWorkArea$Dataset'_'$taskID'_'$iJob.out -J $taskID'_'$iJob $submit $iJob
      done 
      echo $dir $lhe $taskID $nJobs > $iLHE
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
  mv $HOME/LSFJOB_* $LsfOutDir
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
resub=0
addjob=0
sta=0
inj=0
clo=0
cleanlog=0
chknevt=0

nJobMax=0
iJobStart=1

FindFailedJob=0

for arg in $* ; do
  case $arg in
    -tgz)    tgz=$2        ; shift ; shift ;;
    -dir)    dir=$2        ; shift ; shift ;;
    -lhe)    lhein=$2      ; shift ; shift ;;
    -inject) inj=1                 ; shift ;;
    -submit) sub=1                 ; shift ;;
    -njmax)  nJobMax=$2    ; shift ; shift ;;
    -jstart) iJobStart=$2  ; shift ; shift ;;
    -resub)  resub=1               ; shift ;;
    -addjob) addjob=$2     ; shift ; shift ;;
    -status) sta=1                 ; shift ;;
    -fjlist) FindFailedJob=1       ; shift ;;
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

if [ $resub -eq 1 ] ; then
  resub_lhe
  exit
fi

if [ $addjob -gt 0 ] ; then
  add_lhejob
  exit
fi

if [ $sta -eq 1 ] ; then
  sta_lhe
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


