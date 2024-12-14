require "bigdecimal"

def getTecnologia(nome_up)
    if nome_up.start_with?("UC_")
        return "Consumption unit"
    elsif nome_up.start_with?("UPV_") || nome_up.start_with?("UCV_")
        return "Import"
    elsif nome_up.start_with?("UP_DI")
        return "Non-relevant FER"
    elsif @rup_impianti[nome_up] and @rup_impianti[nome_up]["macrotech"]
        return @rup_impianti[nome_up]["macrotech"]
    end
    return "Other"
end

@lista = ["BILATERALISTA", "GRTN BILATERALISTA", "GSE", "GSE SPA"]
@elenco_societa_non_trovate = []
def getSocieta(operatore, nome_up, time)
    o_spazi = operatore.gsub('  ', ' ')
    if @lista.include?(operatore)
        s = getOperatoreByNomeupAndTime(nome_up, time)
        return s if s
        return @rup_impianti[nome_up]["societa"] if @rup_impianti[nome_up] && @rup_impianti[nome_up]["societa"]
        return "UNKNOWN" # Placeholder when no societa is found
    elsif @societa_impianti[operatore]
        return @societa_impianti[operatore]
    elsif @societa_impianti[o_spazi]
        return @societa_impianti[o_spazi]
    end
    @elenco_societa_non_trovate << operatore
    return "UNKNOWN" # Ensure the method always returns a value
end

def getOperatoreByNomeupAndTime(nome_up, time)
        max = nil
        operatore = nil
        if @nomeUpOperatoreHistory[nome_up]
                @nomeUpOperatoreHistory[nome_up].each do |o, t|
                        if max == nil or (t > max and t <= time)
                                max = t
                        end
                        if max <= time
                                operatore = o
                        end
                end
        end
        return operatore
end

def getMacroZona(str)
	mapnomi = {
		"AUST" => "Foreign Countries",
		"BRNN" => "South",
		"CALA" => "South",
		"CALB" => "South",
		"CNOR" => "Central-North",
		"COAC" => "Foreign Countries",
		"CORS" => "Foreign Countries",
		"CSUD" => "Central-South",
		"E_NE" => "Foreign Countries",
		"E_NW" => "Foreign Countries",
		"E_SD" => "Foreign Countries",
		"FOGN" => "South",
		"FRAN" => "Foreign Countries",
		"GREC" => "Foreign Countries",
		"MFTV" => "North",
		"NORD" => "North",
		"PRGP" => "Sicily",
		"ROSN" => "South",
		"SARD" => "Sardinia",
		"SICI" => "Sicily",
		"SLOV" => "Foreign Countries",
		"SUD" => "South",
		"SVIZ" => "Foreign Countries",
		"MALT" => "Foreign Countries",
		"MONT" => "Foreign Countries"
	}
    if !mapnomi.include? str
        puts "nome #{str} not mapped"
        exit 1
    end
    return mapnomi[str]
end

def loadOperatoreSocietaUp(nome_up, operatore, time)
	if time == nil or !nome_up
		return
	end
    if @lista.include?(operatore)
		return
	end
	gruppo=@societa_impianti[operatore]
	if !@nomeUpOperatoreHistory[nome_up]
		@nomeUpOperatoreHistory[nome_up] = {}
		@nomeUpOperatoreHistory[nome_up][gruppo] = time
	elsif @nomeUpOperatoreHistory[nome_up][gruppo] == nil
	    @nomeUpOperatoreHistory[nome_up][gruppo] = time
	else
		t = @nomeUpOperatoreHistory[nome_up][gruppo]
		if time < t
			@nomeUpOperatoreHistory[nome_up][gruppo] = time
		end
	end
end

def getPmax(nome_up)
    if @rup_impianti[nome_up] and @rup_impianti[nome_up]["pmax"]
        return @rup_impianti[nome_up]["pmax"].to_f
    end
    return 0
end

coll = "mgpofferte"
coll_rup = "rup_impianti"
coll_societa = "societa_impianti"
coll_pmax = "mgpofferte_pmax"
coll_aggr = "mgpofferte_aggr"
coll_nomeup_operatore_history = "mgpofferte_up_soc_history"
from = DateTime.parse("2024-09-01 00:00:00")
to = DateTime.parse("2024-10-30 23:59:59")
puts "START FROM #{from.to_s} TO #{to.to_s}"

pars={'data' => {:$gte => from, :$lte => to}}
$storage.del(coll_aggr, pars)
puts "dati cancellati"

@rup_impianti = {}
$storage.find(coll_rup).each do |el|
    impianto = el["IMPIANTO"]
    @rup_impianti[impianto] = {}
    @rup_impianti[impianto]["macrotech"] = el["MACROTECH"]
    @rup_impianti[impianto]["pmax"] = el["PMAX"]
    @rup_impianti[impianto]["societa"] = el["SOCIETA"]
end

@societa_impianti = {}
$storage.find(coll_societa).each do |el|
    @societa_impianti[el["Operatore"]] = el["Gruppo"]
end

@dati_pmax = {}
$storage.find(coll_pmax).each do |el|
    @dati_pmax[el["impianto"]] = el["pmax"]
end

max_offerta = {}
md5 = Digest::MD5.new
pars = { 'bid_offer_date_dt_parsed' => {:$gte => from, :$lte => to}}
row_to_save = {}
c=0
@dati_pmax_temp = {}

###### BEGIN
@nomeUpOperatoreHistory = {}
$storage.find(coll_nomeup_operatore_history).each do |el|
	if !@nomeUpOperatoreHistory[el["nome_up"]]
		@nomeUpOperatoreHistory[el["nome_up"]] = {}
	end
	@nomeUpOperatoreHistory[el["nome_up"]][el["operatore"]] = el["time"]
end
=begin
$storage.find(coll, pars ).each do |el|
    next if (el["status_cd"]!="ACC" && el["status_cd"]!="REJ")
    c = c + 1
	loadOperatoreSocietaUp(el["unit_reference_no"], el["operatore"], el["bid_offer_date_dt_parsed"])
end
p "ecco qua"
#@nomeUpOperatoreHistory.each do |nome_up, el|
@nomeUpOperatoreHistory.each do |nome_up, el|
    @nomeUpOperatoreHistory[nome_up].each do |o, t|
    	#operatore = el.keys[0]
    	#t = el[operatore]
    	item = {:nome_up => nome_up, :operatore => o, :time => t}
            $storage.del(coll_nomeup_operatore_history, item)
            $storage.add(coll_nomeup_operatore_history, item)
    end

end
##### END
=end
p "sono qui"

$storage.find(coll, pars ).each do |el|
    next if el["status_cd"]!="ACC" && el["status_cd"]!="REJ"
    c = c + 1
    row = {}
    d = "#{el["bid_offer_date_dt_parsed"].year}-#{el["bid_offer_date_dt_parsed"].month}-01"
    row["data"] = DateTime.parse(d)
    row["mercato"] = el["market_cd"]
    row["purpose"] = el["purpose_cd"]
    row["nome_up"] = el["unit_reference_no"]
    row["zona"] = el["zone_cd"]
    row["operatore"] = el["operatore"].upcase

    str = row["purpose"] + "-" + row["nome_up"] + "-" + row["data"].strftime('%s')+ "-" + row["zona"]+ "-" + row["operatore"]
    md5.update str
    key = md5.hexdigest
    if !row_to_save[key]
        max_offerta[key] = 0
        row_to_save[key] = row
        row_to_save[key]["offerta"] = 0
        row_to_save[key]["valore_offerta"] = 0
        row_to_save[key]["accettata"] = 0
        row_to_save[key]["valore_accettata"] = 0
        row_to_save[key]["macrotech"] = getTecnologia(row["nome_up"])
        row_to_save[key]["macrozona"] = getMacroZona(row["zona"])
        row_to_save[key]["societa"] = getSocieta(row["operatore"], row["nome_up"], el["bid_offer_date_dt_parsed"])
    end

    if !@dati_pmax_temp[row["nome_up"]]
        @dati_pmax_temp[row["nome_up"]] = 0
    end

    quantity_no = BigDecimal(el["quantity_no"].to_s)
    energy_price_no = BigDecimal(el["energy_price_no"].to_s)
    awarded_quantity_no = BigDecimal(el["awarded_quantity_no"].to_s)
    awarded_price_no = BigDecimal(el["awarded_price_no"].to_s)

    op = row['purpose'] == "BID" ? -1 : 1
    # OFFERTA
    row_to_save[key]["offerta"] = (row_to_save[key]["offerta"] + (quantity_no * op)).to_f
    max_offerta[key] = row_to_save[key]["offerta"] / 744
    pmax = getPmax(row["nome_up"])
    row_to_save[key]["pmax"] = pmax > 0 ? pmax : max_offerta[key]
    if row_to_save[key]["pmax"] > @dati_pmax_temp[row["nome_up"]]
       @dati_pmax_temp[row["nome_up"]] = row_to_save[key]["pmax"]
    end

    # VALORE OFFERTA
    row_to_save[key]["valore_offerta"] = (row_to_save[key]["valore_offerta"] + ((quantity_no * energy_price_no) * op)).to_f

    # ACCETTATA
    row_to_save[key]["accettata"] = (row_to_save[key]["accettata"] + (awarded_quantity_no * op)).to_f

    # VALORE ACCETTATA
    row_to_save[key]["valore_accettata"] = (row_to_save[key]["valore_accettata"] + ((awarded_quantity_no * awarded_price_no) * op)).to_f

    #giacomo
    md5.reset
end

puts "Aggiorno collection #{coll_pmax} PMAX"
@dati_pmax_temp.each do |nome_up, pmax|
    tmp = {:impianto => nome_up, :pmax => pmax}
    if @dati_pmax[nome_up] and @dati_pmax[nome_up] < pmax
        $storage.del(coll_pmax, {:impianto => nome_up})
        $storage.add(coll_pmax, tmp)
    elsif !@dati_pmax[nome_up]
        $storage.add(coll_pmax, tmp)
    end
end

row_to_storage = []
row_to_save.each do |k,value|
    row_to_storage << value
end
colla = $storage.client[coll_aggr]
p "importo i valori"
colla.insert_many(row_to_storage)
#$storage.add(coll_aggr, value)

puts "Società non trovate"
p @elenco_societa_non_trovate.uniq

puts "END"