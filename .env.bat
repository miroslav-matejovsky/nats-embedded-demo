Set-StrictMode -Version Latest

function Enable-LocalEnv {
    $env:SOME_KEY = 'some-value'
}

Enable-LocalEnv
