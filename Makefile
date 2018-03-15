all: build

build:
	@docker build --tag=anthonyrawlinsuom/lfmc-api .
	
install:
	@docker push anthonyrawlinsuom/lfmc-api
	
clean:
	@docker rmi anthonyrawlinsuom/lfmc-api