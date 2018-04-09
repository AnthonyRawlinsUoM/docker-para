all: pull

build:
	@docker build --tag=anthonyrawlinsuom/lfmc-api .
	
install:
	@docker push anthonyrawlinsuom/lfmc-api
	
pull:
	@docker pull anthonyrawlinsuom/lfmc-api

clean:
	@docker rmi --force anthonyrawlinsuom/lfmc-api