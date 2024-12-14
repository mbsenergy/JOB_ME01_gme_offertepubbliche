require 'nokogiri'
require 'date'
require 'net/ftp'
def process_gmeoffers(filename,m)
    doc = Nokogiri::XML(File.open(filename))
    bulk = []
    i = 0
    doc.css("NewDataSet OfferteOperatori").each do |node|
        i += 1
        this = {}
        node.element_children.each do |el|
            val = el.text
            case el.name
            when 'INTERVAL_NO', 'TRANSACTION_REFERENCE_NO', 'MERIT_ORDER_NO'
                val = val.to_i
            when 'BID_OFFER_DATE_DT'
                this[:bid_offer_date_dt_parsed] = Date.parse(val) #Gwydo
            when 'QUANTITY_NO', 'AWARDED_QUANTITY_NO', 'ENERGY_PRICE_NO', 'ADJ_QUANTITY_NO', 'AWARDED_PRICE_NO'
                val = val.to_f
            end
            this[el.name.downcase.to_sym] = val
        end
        this[:time] = this[:bid_offer_date_dt_parsed].to_time + ((this[:interval_no]) * 3600) - ( this[:interval_no] != 25 ?  60*60 :  90*60)
        bulk << {:insert_one => this}
    end
    coll = $storage2.client[m + 'offerte']
    coll.bulk_write(bulk,:ordered => true)
end

def download(m,n = 10,storico = false)
    col = m + 'offerte'
    fil = m + 'opfiles'

    # Obtain what has already been downloaded
    dates = []
    $storage2.find(fil).each {|el| dates << el[:date].to_date}
    
    # How many days back you want to re-save
    #n = 10
    
    Net::FTP.open('download.mercatoelettrico.org') do |ftp|
        ftp.login "PIASARACENO", "18N15C9R"
        ftp.passive = true
        files = ftp.chdir('MercatiElettrici/' + m.upcase + '_OffertePubbliche/')
        files = ftp.list('*')
        n = files.count if storico
        puts "Storico!!!" if storico
        n.times do |i|
            l = !storico ? files.count - (n - i) : i
            filename = files[l].split(" ").last
            next if filename == 'empty'
            date = Date.parse(filename[0..7])
            #Check whether it has already been imported
            if dates.include?(date)
                puts date.to_s + ' already present!'
                next
            end
            puts "Downloading " + filename
            file = ftp.get(filename, "/work/" + filename)
            puts filename + " saved!"
            a = `unzip -l #{filename}`
            filexml = ''
            a.split(' ').each {|e| (filexml = e if e[-4..-1] == '.xml')}
            `unzip #{filename}`
            File.delete(filename)
            puts "Processing file"
            process_gmeoffers(filexml,m)
            $storage2.add(fil,{:date => date}) #gwydo: qui ho tolto la conversione: .to_date
            puts "File processed"
            File.delete(filexml)
        end
    end
    puts "End"
    return true
end


def start(m,n = 10,storico = false,try = 0)
    puts "Tentativo di connessione numero: #{try + 1}"
    download(m,n,storico)
    return true
   rescue
        try += 1
        sleep(5) #sospendi per 10 secondi l'esecuzione (in modo da riprovare la connessione dopo un pò di tempo)
        #start(m,n,storico,try) if try < 4
        try < 5 ? start(m,n,storico,try) : (fail 'Troppi tentativi: connessione non riuscita') #generazione volontaria di un errore per avere la mail che il job ha fallito
    
end

#start(m,n,storico,try)
=begin
#QUESTO METODO SERVE PER FARE PIù TENTATIVI DI CONNESSIONE QUANDO QUESTA FALLISCE 
def ini(storico,n,try = 0)
    puts "Tentativo di connessione numero: #{try + 1}"
    download(storico,n)
    return
rescue
    try += 1
    sleep(5) #sospendi per 10 secondi l'esecuzione (in modo da riprovare la connessione dopo un pò di tempo)
    try < 5 ? ini(storico,n,try) : (fail 'Troppi tentativi: connessione non riuscita') #generazione volontaria di un errore per avere la mail che il job ha fallito
end
ini(storico,n)
=end

