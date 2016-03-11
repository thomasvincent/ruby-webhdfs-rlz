require_relative 'utilities'

module WebHDFS
  class Prueba
    attr_reader :campo

    def initialize(campo)
      self.campo = campo
    end

    def api(path)
      puts WebHDFS.api_path(path)
    end

    def campo=(valor)
      @campo = valor
      puts 'fijado'
    end

    def aqui(valor)
      puts "Aqui: #{valor}"
      valor = mas(valor)
      puts "Aqui: #{valor}"
    end

    def mas(valor)
      valor += 1
      puts "Mas: #{valor}"

      valor
    end
  end
end

p = WebHDFS::Prueba.new(20)

p.api("efren")

a = WebHDFS::Prueba.new(20)

a.api("efren") if a
