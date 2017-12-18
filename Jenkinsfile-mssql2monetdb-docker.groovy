config {
	daysToKeep = 21
	cronTrigger = '@weekend'
	disableGithubPushTrigger = true
}

node() {
	catchError {
		stage("Checkout") {
			git.checkout { }
		}

		stage("Maven package") {
		   	//Deze maven package zorgt ervoor dat de docker:build niet klaagt dat er geen 'target' dir is.
			maven {
				cleanup = false 
				stage = ''
				goals = 'package'
			}               
		}
        
    	stage("Build docker image") {
			maven { 
				cleanup = false
				stage = ''
				options = 'docker:build -DpushImageTag'
				goals = 'package'
			}
		}
	}
    notify {
		slackChannel = "#datazoo-jenkins"
	}
}