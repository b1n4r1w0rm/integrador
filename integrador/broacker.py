#!/usr/bin/python

#Informacoes necessarias
#criar diretorios no storage por empresa
#gravar tudo os backups no diretorio por empresa



from paramiko import SSHClient
import paramiko
import pika
import time


class SSH:
    def __init__(self):
        self.ssh = SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(hostname='10.2.40.171',username='root',password='ggpisp123123@#')

    def exec_cmd(self,cmd):
        stdin,stdout,stderr = self.ssh.exec_command(cmd)
        if stderr.channel.recv_exit_status() != 0:
            print stderr.read()
        else:
            print stdout.read()



#fileset create 001010102200202A 0 /home /backup /ISO
#fileset delete 001010102200202A /home /ISO
#fileset change  001010102200202A /var/db /backup /home/bks /home
#fileset get  001010102200202A
def fileset(filesets):
   file = filesets
   print "fileset" 
   

#schedule create  001010102200202A "schedule_teste" run="Full 1st Sat at 23:00" 
#schedule delete  001010102200202A
#*obs: validar se nenhum job esta usando esta schedule
#schedule change  001010102200202A  run=Full 1st Sat at 21:00
#schedule get    001010102200202A
def schedule(schedules):
   file = schedules
   print "schedules"


#pool create  001010102200202A-7D
#*configure add pool name="001010102200202A-7D" pooltype=backup recycle=yes autoprune=yes volumeretention=7days maximumvolumebytes=1G labelformat=001010102200202A-7D-Diario
#pool create  001010102200202A-2M
#*configure add pool name="001010102200202A-2M" pooltype=backup recycle=yes autoprune=yes volumeretention=2months maximumvolumebytes=1G labelformat=001010102200202A-2M-Mensal
#pool delete  001010102200202A-2M
#Selecionar todos os pools criados e fazer purge e delete deles via shell
#pool change 001010102200202A-2M 4M
#Renomear o gid para  001010102200202A-4M e alterar o valor de 2 months para 4 months no arquivo
#pool get  001010102200202A-2M
def pool(pools):
   file=pools
   print "pools"
   #empresa GID para criar os diretorios de backup

#job create  001010102200202A  001010102200202A-7D  001888888882A  001010999999202A  001010102200202A
#*configure add job name="001010102200202A" type=Backup messages="Standard" storage="File" pool="001010102200202A-7D"  client="001888888882A" 
#fileset="001010999999202A" schedule="001010102200202A" jobdefs="DefaultLinux" writebootstrap="/var/lib/bareos/%c.bsr" maximumconcurrentjobs=30 accurate=yes enable=yes catalog=9999999999999

#job delete  001010102200202A
#Lista todos os pools e faz o purge e apaga do disco

#job change  001010102200202A disable/enable
#altera enable=yes para enable=no e faz reload

#job getstatus 9999999999999 fatalerror

#bconsole  <<EOF
#use catalog=9999999999999 
#llist jobs jobstatus=f
#quit  
#EOF

#Retornar numero de quantos backups com erro fatal (nao foi feito backup)
#sh command_bconsole.sh |grep "\bJobId\b" |wc -l

#job listjobs 9999999999999 fatalerror
#Retorno a lista com todos os jobs de determinado erro

#job jobstatus 9999999999999 15

#sh command_bconsole.sh |grep -E 'Time|LogText|Scheduled time|Start time|End time|Elapsed time'
#Mensagem de erro com time de um jobid=15
#[root@dir01 ~]# sh command_bconsole.sh |grep -E 'Time|LogText'
#    Time: 2018-03-30 21:32:00
# LogText: bareos-dir JobId 15: shell command: run BeforeJob "/usr/lib/bareos/scripts/make_catalog_backup.pl MyCatalog"
#    Time: 2018-03-30 21:32:00
# LogText: bareos-dir JobId 15: Start Backup JobId 15, Job=BackupCatalog.2018-03-30_21.10.00_05
#    Time: 2018-03-30 21:36:25
# LogText: bareos-dir JobId 15: Warning: bsock_tcp.c:128 Could not connect to Storage daemon on stg01.bks.com.br:9103. ERR=Connection timed out
#    Time: 2018-03-30 22:02:08
# LogText: bareos-dir JobId 15: Fatal error: bsock_tcp.c:134 Unable to connect to Storage daemon on stg01.bks.com.br:9103. ERR=Interrupted system call
#    Time: 2018-03-30 22:02:08
# LogText: bareos-dir JobId 15: Error: Bareos bareos-dir 17.2.4 (21Sep17):
#  Volume Session Time:    0

#[root@dir01 ~]# cat command_bconsole.sh 
#bconsole <<EOF 
#use catalog=MyNewCatalog
#llist joblog jobid=15   
#quit  
#EOF

#Job Status Code 	Meaning 
#A 	Canceled by user 
#B 	Blocked 
#C 	Created, but not running 
#c 	Waiting for client resource 
#D 	Verify differences 
#d 	Waiting for maximum jobs 
#E 	Terminated in error 
#e 	Non-fatal error 
#f 	fatal error 
#F 	Waiting on File Daemon 
#j 	Waiting for job resource 
#M 	Waiting for mount 
#m 	Waiting for new media 
#p 	Waiting for higher priority jobs to finish 
#R 	Running 
#S 	Scan 
#s 	Waiting for storage resource 
#T 	Terminated normally 
#t 	Waiting for start time 


def job(jobs):
   file = jobs
   print "jobs"


def create():
   print "def create"

#Funcao Device
#device create 43843893993A teste_123 10.2.40.222 123123qwe 999999999 
#Comando enviado ao Bareos:
#configure  add client name= 43843893993A description="teste_123" address=10.2.40.222 password="123123qwe" catalog=999999999 connectionfromdirectortoclient=no connectionfromclienttodirector=yes heartbeatinterval="1 minutes" autoprune=yes maximumconcurrentjobs=30

#device delete 43843893993A
#faz purge dos dados e apaga volumes

#device change 43843893993A enable/disable
#Habilita ou desabilita o device

#device getstatus 43843893993A
#Retorno os seguintes status:
#A1: ativo e online
#A0: ativo e offline
#D0: desativado
def device(devices):
   file = devices
   print devices
   if devices[1] == "create":
      print "create devices"
      ssh = SSH()
      cmdssh = "configure add client name="+devices[2]+" description="+devices[3]+" address="+devices[4]+" password="+devices[5]+" catalog="+devices[6]+" connectionfromdirectortoclient=no connectionfromclienttodirector=yes autoprune=yes maximumconcurrentjobs=30 heartbeatinterval=20"
      #print cmdssh
      ssh.exec_cmd("echo "+cmdssh+">> /root/teste.log")
      ssh.exec_cmd("echo "+cmdssh+"| bconsole")
      #create()
   elif devices[1] == "delete":
      print "delete devices"
   elif devices[1] == "change":
      print "change devices"
      

   elif devices[1] == "getstatus":
      print "retorna status"
   else:
      print "opcao nao encontrada"   


def storage(storages):
   file = storages
   print "storages"


def catalog(catalogs):
   file = catalogs
   print "empresa"
   #Identificacao | acao | gid (software guarda) | dbname | dbuser | dbpass | ID empresa | nome da empresa | nome_do_storage (onde esta o SD)
   if catalogs[1] == "create":
      print "create catalog"
      ssh = SSH()
      cmdssh = "configure add catalog name="+catalogs[2]+" dbname="+catalogs[3]+" dbuser="+catalogs[4]+" dbpassword="+catalogs[5]+" dbdriver=mysql multipleconnections=yes"
      print cmdssh
      ssh.exec_cmd("echo "+cmdssh+">> /root/teste.log")
      ssh.exec_cmd("echo "+cmdssh+"| bconsole")
      #create()
   elif devices[1] == "delete":
      print "delete catalog"
   else:
      print "option catalog not found !!"
 




#Falta primeira parte tamanho e status do dir





#if __name__ == '__main__':
#    ssh = SSH()
#    ssh.exec_cmd("echo "'configure add catalog name="teste123" dbpassword="123qwe" dbuser="teste123" dbname="teste123" dbdriver="postgresql"'" > /root/teste.log")


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

def callback(ch, method, properties, body):
    #print(" %r" % body)
    #print body
    
    lista = body.split(" ")

    if lista[0] == "fileset":
       fileset(lista)
    elif lista[0] == "schedule":
       schedule(lista)
    elif lista[0] == "pool":
       pool(lista)
    elif lista[0] == "job":
       job(lista)
    elif lista[0] == "device":
       device(lista)
    elif lista[0] == "storage":
       storage(lista)
    elif lista[0] == "catalog":
       catalog(lista)
    else:
       print "opcao nao encontrada"

    time.sleep(body.count(b'.'))
    #print(" [x] Done")
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='task_queue')

channel.start_consuming()


try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
connection.close()
