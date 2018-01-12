#!/bin/bash

# global info
JOBNAME='anonymous'
REGISTRY='in.fds.so:5000'

VERSION=1

KMR_BASE_IMAGE_NAME=in.fds.so:5000/kmr${VERSION}_base
KMR_BASE_IMAGE_TAG=latest
# KMR_BASE_IMAGE_NAME=golang
# KMR_BASE_IMAGE_TAG=1.8.5
KMR_BASE_IMAGE_FULLNAME=${KMR_BASE_IMAGE_NAME}:${KMR_BASE_IMAGE_TAG}

IFS='' read -r -d '' SOURCE_LIST <<'EOF'
  deb http://mirrors.aliyun.com/ubuntu/ zesty main restricted universe multiverse
  deb http://mirrors.aliyun.com/ubuntu/ zesty-security main restricted universe multiverse
  deb http://mirrors.aliyun.com/ubuntu/ zesty-updates main restricted universe multiverse
  deb http://mirrors.aliyun.com/ubuntu/ zesty-proposed main restricted universe multiverse
EOF
SOURCE_LIST_FILE=/tmp/sourcelist

echo "$SOURCE_LIST" >${SOURCE_LIST_FILE}

IFS='' read -r -d '' KMR_BASE_IMAGE_DOCKERFILE <<'EOF'
FROM golang:1.8.5
RUN apt-get -y update
RUN apt-get -y install librados-dev
ENV LD_LIBRARY_PATH=/lib:/usr/lib:/usr/local/lib
EOF

LOG_LEVEL=2
_log() {
    local prefix=$1
    local level=$2
    local msg=${@:3}
    prefix="$(date +'%T') $prefix"
    if [ $level -le $LOG_LEVEL ]; then
        IFS=$'\n'
        for x in $msg; do
            echo $prefix $x
            prefix=$'\t'
        done
        unset IFS
    fi
}
debug() {
    _log "[Debug]" 3 $@
}
info() {
    _log "[Info]" 2 $@
}
warning() {
    _log "[Warning]" 1 $@
}
error() {
    _log "[Error]" 1 $@
    exit
}
r() {
    debug "Run command: " $@
    $@
}

[[ ! $GOPATH || ! -d $GOPATH ]] && error "Please set GOPATH correctly"

images=$(docker images | awk \
    '$1 == '"\"${KMR_BASE_IMAGE_NAME}\""' && $2 == '"\"${KMR_BASE_IMAGE_TAG}\""'{print $1":"$2}')
if [ -z $images ]; then
    # build base image
    info "No base image [${KMR_BASE_IMAGE_FULLNAME}] found, building now"
    debug $'Dockerfile content is:\n' $KMR_BASE_IMAGE_DOCKERFILE
    echo "$KMR_BASE_IMAGE_DOCKERFILE" | docker build -t ${KMR_BASE_IMAGE_FULLNAME} -
else
    debug "Base image found: " $images
fi

CONTAINER_NAME=''
IN_TRANSACTION=0
COMMAND_FILE=/tmp/commands.sh
clean() {
    r docker rm $CONTAINER_NAME || error 'failed to clean container in commit phase'
}

begin() {
    [ $IN_TRANSACTION = 1 ] && error "Transaction nested!"
    IN_TRANSACTION=1

    debug "Begin command transaction"
    CONTAINER_NAME="${JOBNAME}$(date +'%s')"
    cat /dev/null >${COMMAND_FILE}
}
execute() {
    [ $IN_TRANSACTION = 0 ] && error "Transaction not started before execute!"
    debug "Run command in container: " $@
    echo $@ >>${COMMAND_FILE}
}
commit() {
    local baseImageName=$1
    local commitImageName=$2
    local dockerArgs=${@:3}

    local containerLabel='com.naturali.kmrdeploy='$JOBNAME

    debug "baseImageName is $baseImageName"
    debug "commitImageName is $commitImageName"
    debug "dockerArgs is $dockerArgs"

    [ $IN_TRANSACTION = 0 ] && error "Transaction not started before commit!"

    [ -z $commitImageName ] && debug "Commit image name is empty, only run command" ||
        info "Commit image name is [$commitImageName]"

    debug "Commit transaction... " $'\n'
    debug $'Script contents is\n' $(cat ${COMMAND_FILE})
    info "Container name is ${CONTAINER_NAME}, label is ${containerLabel}"

    r docker run --name "$CONTAINER_NAME" --label "${containerLabel}" \
        $([ -z "$commitImageName" ] && echo '--rm') \
        -v $SOURCE_LIST_FILE:/etc/apt/source.list \
        -v $COMMAND_FILE:$COMMAND_FILE \
        ${dockerArgs[@]} $baseImageName /bin/bash "$COMMAND_FILE" &&
        info "Command finished in container ${CONTAINER_NAME}, now commit to image" ||
        { { [ ! -z "$commitImageName" ] && clean; }; error "Failed to run commands in container"; }

    [ -z $commitImageName ] && debug 'Skip commit this image without name' ||
        { r docker commit $CONTAINER_NAME $commitImageName &&
            { info "Image [${commitImageName}] commit succeed!" && clean; } ||
            { clean && error "Failed to commit container"; }; }

    IN_TRANSACTION=0
}

NO_GLOBAL_CONFIG=0

buildJobImage() {
    local baseImageName=$1
    local imageName=$2
    local mainFile=$(realpath "$3")
    local configFiles=$4 # split by colon (:)
    local assetsFolder=$([ ! -z "$5" ] && realpath "$5")
    local outputBinaryFile=$6
    local outputConfigFile=$7
    local outputAssetsFolder=$8

    local gosrcPath=$(realpath "$GOPATH")/src
    debug "Enter buildJobImage..."
    debug "imageName: $imageName"
    debug "mainFile: $mainFile"
    debug "configFiles: $configFiles"
    debug "assetsFolder: $assetsFolder"

    debug "outputBinaryFile: $outputBinaryFile"
    debug "outputConfigFile: $outputConfigFile"
    debug "outputAssetsFolder: $outputAssetsFolder"

    debug "gosrcPath: $gosrcPath"

    [[ ! "$mainFile" =~ ^"$gosrcPath" ]] && error "Job's main file should be in \$GOPATH/src"
    [[ (! -z $assetsFolder) && (! "$assetsFolder" =~ ^"$gosrcPath") ]] && error "Job's assets folder should be in \$GOPATH/src"
    [ -z $outputBinaryFile ] && error 'OutputBinaryFile should not be empty'
    [ -z $outputConfigFile ] && error 'OutputConfigFile should not be empty'

    local confAbsPathsInContainer
    IFS=:
    for x in $(echo $configFiles); do
        local f=$(realpath $x)
        [[ ! "$f" =~ ^"$gosrcPath" ]] && error "Job's config file $f should be in \$GOPATH/src"
        confAbsPathsInContainer=$confAbsPathsInContainer"/go${f#$GOPATH} "
    done
    unset IFS

    local binAbsPathInContainer=/go${mainFile#$GOPATH}
    local assetsAbsPathInContainer=/go${assetsFolder#$GOPATH}
    info "Absolute path of main file in container is: ${binAbsPathInContainer}"

    begin
    execute mkdir -p \$\(dirname $outputBinaryFile\)
    execute mkdir -p \$\(dirname $outputConfigFile\)
    [ ! -z $outputAssetsFolder ] && execute mkdir -p \$\(dirname $outputAssetsFolder\)

    if [[ ! -z $assetsFolder &&  -d $assetsFolder ]];then
        execute echo "Copying assets folder"
        execute cp -r $assetsAbsPathInContainer $outputAssetsFolder '|| exit 127'
        execute echo "Copying assets folder done"
    fi

    execute echo "Building source file" $binAbsPathInContainer
    execute \(cd \$\(dirname $binAbsPathInContainer\)\; go build -i -o $outputBinaryFile $binAbsPathInContainer\) '|| exit 127'
    execute echo "Gen config file" $outputConfigFile
    execute $outputBinaryFile \
        $([ ! -z $confAbsPathsInContainer ] && echo --config $confAbsPathsInContainer) \
        internal-print-config \
        --output $outputConfigFile '|| exit 127'
    execute echo "Config file content is"
    execute ls -lh $outputConfigFile
    execute cat $outputConfigFile


    local mounts=()
    mounts+=(-v "$GOPATH:/go")
    [ $NO_GLOBAL_CONFIG = 0 ] &&
        { mounts+=(-v /etc/kmr:/etc/kmr); mounts+=(-v ~/.config/kmr:/root/.config/kmr); }
    commit "$KMR_BASE_IMAGE_FULLNAME" "$imageName" ${mounts[@]}

    debug "Leave buildJobImage"
}

deployJobImage() {
    local imageName=$1
    local binFile=$2
    local confFile=$3
    local assetFolder=$4
    local deployArgs=${@:5}

    # check parameters ?
    info "Deploying..."

    begin
    execute rm -rf /etc/kmr \$HOME/.config/kmr
    execute $binFile --config $confFile \
        internal-deploy \
        --binary $binFile \
        --image-name $imageName \
        --work-dir $assetFolder \
        $deployArgs
    commit $imageName '' \
        -v $HOME/.kube:/root/.kube

    info "Deployed"
}

deployLocal() {
    local mainFile=$1
    local configFiles=$2
    local assetsFolder=$3
    local args=${@:4}

    local image=${REGISTRY}/${JOBNAME}:$(date +%s)
    local bin=/kmr/main
    local config=/kmr/config.json
    local asset=/kmr/assets
    buildJobImage "$KMR_BASE_IMAGE_FULLNAME" "$image" \
     "$mainFile" "$configFiles" "$assetsFolder" "$bin" "$config" "$asset" &&
    deployJobImage "$image" "$bin" "$config" "$asset" $args &&
        { r docker push $image || (error "Failed to push image [$image]"); }
    r docker rmi $image
}

deployGit() {
    local gitUrl=$1
    local mainFile=$2
    local configFile=$3
    local assetsFolder=$4
    local args=${@:5}

    local srcPath=$(echo "$gitUrl" | sed -e 's/^https\?:\/\///')
    debug 'gitUrl is: ' $gitUrl
    debug 'srcPath is: ' $srcPath
    
    local projPath="$GOPATH/src/$srcPath"
    debug 'Project path is: ' $projPath

    r go get -d -insecure $srcPath
    [ -f ${projPath}/${mainFile} ] && 
        deployLocal "$projPath/$mainFile" "$projPath/$configFile" "$projPath/$assetsFolder" $args ||
        error "Failed to get $gitUrl"
}

help() {
    read -r -d '' HELP <<EOF
Command:
    $(basename $0) [giturl] MainFile [assetsFolder] [flags]
    NOTE: 
        if giturl is specified, all file path should be relative to root directory 
        of the git project. Otherwise, both absolute and relative path is ok.
Description:
    Deploy a kmr job.
Flags:
    --no-global-config
        Do not use global config(/etc/kmr and ~/.config/kmr).
    --registry
        Set registry of docker image
    --install, -i [PACKAGES split by colon(:)]
        NYI!!!
        Install PACKAGES before compile.
    --jobname [STRING]
        Specify jobname, used to determine image name. Default: anonymous.
    --config [FILES split by colon(:)]
        Specify job-specified config files.
    --port [NUM]
        Port of master
    --worker-num [NUM] 
        Number of workers
    --cpu-limit [NUM]
        CPU limit of workers
    --check-point [PATH]
        Check porint file
    --fresh-run
        Fresh run, ignore checkpoint.
    --max-retries
        If a task fails more than MAX-RETRIES, it\'ll be regarded as succeeded. <=0 mean unlimited,
        Default: 0
    --v
        output verbose
    --vv
        output more verbose
EOF
    echo "$HELP"
}

if [[ $# < 1 ]];then
    help
    exit 0
fi

main() {
    local gitUrl mainFile assetsFolder configFile args=()
    local gitDeploy=0
    [[ "$1" =~ ^https?:// ]] && { gitDeploy=1; gitUrl=$1; shift; }
    [[ $1 != -* ]] && { mainFile=$1; shift; }
    [[ $1 != -* ]] && { assetsFolder=$1; shift; }

    [ -z "$mainFile" -o $gitDeploy = 0 -a ! -f "$mainFile" ] && 
        { help; error "Main file $mainFile not exists"; }

    while (( "$#" ));do
        case $1 in
            --install|-i)
            # install a package before compile
            error "--install|-i not implemented"
            ;;
            --jobname)
            [ ! -z $2 ] && JOBNAME=$2 || { error "Jobname should not be empty"; }
            shift;shift
            ;;
            --config)
            configFile=$2
            shift;shift
            ;;
            --port|--worker-num|--cpu-limit|--max-retries)
            # simple check, not enough
            [[ $2 =~ ^[0-9]+$ ]] && args+=($1 $2) || { help && error 'NUM arg required for ' $1; }
            shift;shift
            ;;
            --check-point)
            args+=($1 $2)
            shift;shift
            ;;
            --fresh-run)
            args+=($1)
            shift
            ;;
            -v|-vv|--v|--vv)
            # xxx
            LOG_LEVEL=3
            shift
            ;;
            --help|-h)
            help
            exit 0
            ;;
            --no-global-config)
            NO_GLOBAL_CONFIG=1
            shift
            ;;
            --registry)
            [[ $2 != -* && ! -z $2 ]] && REGISTRY=$2 || { help && error 'Registry format error' $2; }
            shift;shift
            ;;
            *)
            help
            error "Unkown flag $1"
            ;;
        esac
    done
    
    debug "VERSION:" $VERSION
    debug "LOG_LEVEL:" $LOG_LEVEL
    debug "REGISTRY:" $REGISTRY
    debug "NO_GLOBAL_CONFIG:" $NO_GLOBAL_CONFIG

    if [ $gitDeploy = 1 ];then
        deployGit "$gitUrl" "$mainFile" "$configFile" "$assetsFolder" ${args[@]}
    else
        deployLocal "$mainFile" "$configFile" "$assetsFolder" ${args[@]}
    fi
}

main $@

