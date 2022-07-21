all: data_1k_1g.nq data_1k_10g.nq data_1k_100g.nq data_1k_1000g.nq data_10k_1g.nq data_10k_10g.nq data_10k_100g.nq data_10k_1000g.nq data_100k_1g.nq data_100k_10g.nq data_100k_100g.nq data_100k_1000g.nq data_1000k_1g.nq data_1000k_10g.nq data_1000k_100g.nq data_1000k_1000g.nq data_1k_1g.nq.delete data_1k_10g.nq.delete data_1k_100g.nq.delete data_1k_1000g.nq.delete data_10k_1g.nq.delete data_10k_10g.nq.delete data_10k_100g.nq.delete data_10k_1000g.nq.delete data_100k_1g.nq.delete data_100k_10g.nq.delete data_100k_100g.nq.delete data_100k_1000g.nq.delete data_1000k_1g.nq.delete data_1000k_10g.nq.delete data_1000k_100g.nq.delete data_1000k_1000g.nq.delete

%.nq:
	sg=$$(echo $@ | sed -E 's/data_([[:digit:]]+)k_([[:digit:]]+)g.nq/\1 \2/'); statements=$$(echo $$sg | cut -d ' ' -f 1); graphs=$$(echo $$sg | cut -d ' ' -f 2); cd .. && clj -J-Xss16m -M -m data-gen.main --statements $${statements}000 --graphs $${graphs} --output-file data/$@

%.nq.delete: %.nq
	s=$$(echo $@ | sed -E 's/data_([[:digit:]]+)k_.*/\1/'); to_delete=$$(( s * 500 )); head -n $$to_delete $*.nq > $@

# 1k statements
data_1k_1g.nq:
data_1k_10g.nq:
data_1k_100g.nq:
data_1k_1000g.nq:

data_1k_1g.nq.delete:
data_1k_10g.nq.delete:
data_1k_100g.nq.delete:
data_1k_1000g.nq.delete:


# 10k statements
data_10k_1g.nq:
data_10k_10g.nq:
data_10k_100g.nq:
data_10k_1000g.nq:

data_10k_1g.nq.delete:
data_10k_10g.nq.delete:
data_10k_100g.nq.delete:
data_10k_1000g.nq.delete:


# 100k statements
data_100k_1g.nq:
data_100k_10g.nq:
data_100k_100g.nq:
data_100k_1000g.nq:

data_100k_1g.nq.delete:
data_100k_10g.nq.delete:
data_100k_100g.nq.delete:
data_100k_1000g.nq.delete:

# 1m statements
data_1000k_1g.nq:
data_1000k_10g.nq:
data_1000k_100g.nq:
data_1000k_1000g.nq:

data_1000k_1g.nq.delete:
data_1000k_10g.nq.delete:
data_1000k_100g.nq.delete:
data_1000k_1000g.nq.delete:

clean:
	rm *.nq
	rm *.nq.delete
