all:
	g++ -O2 -std=c++11 generate_csv.cpp -o generate_csv
clean:
	rm -f generate_csv
