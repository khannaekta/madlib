pipeline {

agent {
        node {
            label 'madlib'
        }
    }
stages {
    stage ('ExecuteTestScript') {
        steps {
            // Load script from package path
            //def script_bash = libraryResource 'tool/jenkins/'

            // create a file with script_bash content
            // writeFile file: './test.sh', text: script_bash

            // Run it!
            sh 'bash ./tool/jenkins/rat_check.sh'
        }
    }
}
}
