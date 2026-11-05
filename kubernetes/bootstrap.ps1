param([string]$RunId = (Get-Date -Format 'yyyyMMdd-HHmmss'))
$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent
$toolDir = if ($env:K8S_TOOLS) { $env:K8S_TOOLS } else { 'C:\rustfs-lab\k8s-tools' }
New-Item -ItemType Directory -Force $toolDir | Out-Null
$env:K8S_TOOLS = $toolDir
$env:LAB_RUN = $RunId
$env:PYTHONIOENCODING = 'utf-8'

function Invoke-Checked {
    param([string]$Program, [string[]]$Arguments)
    & $Program @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$Program failed: $LASTEXITCODE" }
}

function Get-VerifiedBinary {
    param([string]$Name, [string]$Url, [string]$HashUrl)
    $destination = Join-Path $toolDir $Name
    $hashFile = "$destination.sha256"
    Invoke-Checked 'curl.exe' @('-fsSL', '--retry', '2', '--max-time', '180', '-o', $destination, $Url)
    Invoke-Checked 'curl.exe' @('-fsSL', '--retry', '2', '--max-time', '60', '-o', $hashFile, $HashUrl)
    $expected = ((Get-Content $hashFile -Raw).Trim() -split '\s+')[0]
    if ((Get-FileHash $destination -Algorithm SHA256).Hash -ne $expected) { throw "$Name checksum mismatch" }
}

Get-VerifiedBinary 'kind.exe' 'https://kind.sigs.k8s.io/dl/v0.33.0/kind-windows-amd64' 'https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-windows-amd64.sha256sum'
Get-VerifiedBinary 'kubectl.exe' 'https://dl.k8s.io/release/v1.35.8/bin/windows/amd64/kubectl.exe' 'https://dl.k8s.io/release/v1.35.8/bin/windows/amd64/kubectl.exe.sha256'
Invoke-Checked 'docker' @('info', '--format', '{{.ServerVersion}}')
$kind = Join-Path $toolDir 'kind.exe'
$kubectl = Join-Path $toolDir 'kubectl.exe'
$kubeconfig = Join-Path $toolDir 'devops-kubeconfig'
$clusters = & $kind get clusters
if ($LASTEXITCODE -ne 0) { throw 'Cannot enumerate kind clusters' }
if ($clusters -notcontains 'devops-k8s') {
    Invoke-Checked $kind @('create','cluster','--name','devops-k8s','--config',"$PSScriptRoot\kind.yaml",'--kubeconfig',$kubeconfig,'--wait','180s')
} elseif (-not (Test-Path $kubeconfig)) {
    Invoke-Checked $kind @('export','kubeconfig','--name','devops-k8s','--kubeconfig',$kubeconfig)
}
Invoke-Checked 'docker' @('update','--cpus','4','--memory','5g','--memory-swap','5g','devops-k8s-control-plane')
# Direct containerd pull avoids missing-platform manifests in Docker Desktop exports.
Invoke-Checked 'docker' @('exec','devops-k8s-control-plane','ctr','-n','k8s.io','images','pull','--platform','linux/amd64','docker.io/rustfs/rustfs:1.0.0')
Push-Location $repo
try {
    $python = Join-Path $repo '.venv\Scripts\python.exe'
    if (-not (Test-Path $python)) {
        Invoke-Checked 'python' @('-m','venv','.venv')
        Invoke-Checked $python @('-m','pip','install','-r','kubernetes/requirements.txt')
    }
    Invoke-Checked 'docker' @('build','-f','kubernetes/Dockerfile','-t','devops-mapreduce:k8s','.')
    Invoke-Checked $kind @('load','docker-image','devops-mapreduce:k8s','--name','devops-k8s')
    foreach ($stage in @('setup','baseline','migrate','compute','persistence')) {
        Invoke-Checked $python @('kubernetes/run.py',$stage)
    }
    Invoke-Checked $python @('kubernetes/scheduling.py')
    Invoke-Checked $python @('kubernetes/audit.py')
} finally { Pop-Location }
