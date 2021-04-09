# receptor Reliable UDP
from helper import *
from argparse import ArgumentParser
import socket
import logging
import random
import _thread
from threading import Thread, Lock
import select
import time

parser = 0                          # the parser that we use to get the arguments
last_sequence_number = 0            # holds the last sequence number received from emitter
seq_nr_to_payload = {}              # dictionary that maps sequence numbers to the payload of that packet
writing_offset = 0                  # writing offset for the file we're writing in
queue = []                          # queue to store the received packets before checking them and sending acknowledgements 
exit = False                        # a boolean that marks when we want to exit the program
g = 0                               # file descriptor of output file
number_of_invalid_packets = 0       # counter that holds the number of invalid packets we receive
number_of_packets_received = 0      # counter that holds the number of packets received

writing_mutex = Lock()              # mutex for seq_nr_to_payload dictionary
queue_mutex = Lock()                # mutex for the queue we're using for the queue
window_mutex = Lock()               # mutex for the last window size
exit_mutex = Lock()                 # mutex that marks when we want to stop the receiving process

receiving_thread = 0                # a thread that receives packets
write_thread = 0                    # a thread that writed to the output file

window_mutex.acquire()
window = random.randint(1, 5)       # the last window size
window_mutex.release()


logging.basicConfig(format = u'[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', level = logging.NOTSET)

# receives connection from the emitter based on a received packet
def receive_connection(sock, emitter_address, data):
    global window, last_sequence_number
    global number_of_invalid_packets, number_of_packets_received

    seq_nr, checksum, flags = parse_header_emitter(data)

    if flags != 'S': 
        logging.info("Received packet didn't have flag 'S'.")                                                                                       # S means that we want to start the connection
        return False

    header_fara_checksum = create_header_receiver(cut_sequence_number(seq_nr + 1), 0, window)
    
    last_sequence_number = cut_sequence_number(seq_nr)

    checksum = calculeaza_checksum(header_fara_checksum)

    header_cu_checksum = create_header_receiver(cut_sequence_number(seq_nr + 1), checksum, window)

    logging.info(f"Sending an answer to the connection request.\n")
    sock.sendto(header_cu_checksum, emitter_address)                                                        # we send the acknowledgement number
    return True                                                                                             # returns True if the connection was established

def finalize_connection(sock):                                                                              # finalizes the connection by marking exit as true
    global exit_mutex, exit
    global receiving_thread, write_thread
    global number_of_invalid_packets, number_of_packets_received

    logging.info("Closing the connection.")
    p = (number_of_invalid_packets/number_of_packets_received) * 100
    logging.info("'{0}' % corruption of packets from emitter.".format(p))
    exit_mutex.acquire()
    exit = True
    exit_mutex.release()

# a thread that deals with special messages sent as a result of a 0 sized window
def window_0(sock, emitter_address):                                                                       
    global window_mutex, exit, exit_mutex, window
    global number_of_invalid_packets, number_of_packets_received
    global queue, queue_mutex

    logging.info("Started looking for 0 windows.")
    exit_mutex.acquire()
    while exit==False:
        exit_mutex.release()

        window_mutex.acquire()
        while window == 0:   
            window_mutex.release()            
            time.sleep(0.5)                                                         # we sleep to simulate a real-world situation

            queue_mutex.acquire()
            while not queue:
                queue_mutex.release()

                w = random.randint(0, 2)                                                # we generate a random window                                         
                header_fara_checksum = create_header_receiver(max_nr-1, 0, w)           # the sequence_number saved for this special packet is max_nr-1 
                checksum = calculeaza_checksum(header_fara_checksum)
                header_cu_checksum = create_header_receiver(max_nr-1, checksum, w)

                logging.info(f"Sending a special packet with window {w}.")
                sock.sendto(header_cu_checksum, emitter_address)                        # we send this special packet 

                time.sleep(1)                                                           #giving emitator time to resume sending packets

                queue_mutex.acquire()

                exit_mutex.acquire()
                if exit:
                    exit_mutex.release()
                    break
                exit_mutex.release()

            queue_mutex.release()

            window_mutex.acquire()
            window=w                                                                # we also update the last window size sent

        window_mutex.release()
        exit_mutex.acquire()
    exit_mutex.release()
    quit()



# this thread deals with received packets in the queue
def deal_with_received_packets(sock):

    logging.info("We start dealing with payload packets.")
    global g, writing_offset, seq_nr_to_payload, last_sequence_number, exit_mutex, receiving_thread, window, window_mutex
    global number_of_packets_received, number_of_invalid_packets

    exit_mutex.acquire()
    while exit==False:
        exit_mutex.release()
        packet = 0
        queue_mutex.acquire()
        if len(queue)>0:
            packet = queue[0]                                                           # we get a packet from queue
            queue.pop(0)
        queue_mutex.release()

        if packet!=0:
            data = packet[0]
            emitter_address = packet[1]
            seq_nr, checksum, flags = parse_header_emitter(data)

            logging.info('Received packet with seq_nr "%d"', seq_nr)

            if verifica_checksum(data):
                
                if flags == 'F':
                    logging.info("Packet has flag 'F'.")                                                        # if the received packet has flag F, we finalize the connection
                    finalize_connection(sock)
                    exit_mutex.acquire()
                    exit_mutex.release()
                else:

                    writing_mutex.acquire()
                    if seq_nr >= len(get_payload(data)):
                        seq_nr_to_payload[seq_nr-len(get_payload(data))]=get_payload(data)
                    else:                                                                                   # we address the case: seq_nr = 102, len(data) = 200
                        seq_nr_to_payload[seq_nr + (max_nr-1)-len(get_payload(data))] = get_payload(data)       
                    writing_mutex.release()
                    
                    if flags!='P':
                        logging.info("Packet had flag 'S'")
                        return

                    logging.info("Packet has flag 'P'.")
                    seq_nr = (seq_nr+1) % (max_nr-1)                                    # we add 1 to the seq_nr to get the ack_nr

                    window_mutex.acquire()
                    window = random.randint(0, 6)
                    window_mutex.release()
                    header_fara_checksum = create_header_receiver(seq_nr, 0, window)

                    checksum = calculeaza_checksum(header_fara_checksum)

                    header_cu_checksum = create_header_receiver(seq_nr, checksum, window)

                    logging.info("Sending acknowledgment for packet with sequence number '%d'", seq_nr )
                    sock.sendto(header_cu_checksum, emitter_address)                    # we send the packet as acknowledgment

            else:
                logging.info("Checksum for packet is not valid, ignoring it...")
                number_of_invalid_packets+=1

        exit_mutex.acquire()

    exit_mutex.release()
    logging.info("Closing receive_thread.")
    quit()

    
# a threads that waits for the expected packet and writed to output
def write_to_output():
    global seq_nr_to_payload, last_sequence_number, g, writing_offset, exit_mutex, write_thread, g
    
    logging.info("Started looking for payload to write to output file.")
    max_nr = 1<<16
    exit_mutex.acquire()
    while exit==False:
        exit_mutex.release()
        writing_mutex.acquire()
        b = last_sequence_number in seq_nr_to_payload.keys()                                # we check if the expected packet has arrived
        writing_mutex.release()
        if b==True:
            writing_mutex.acquire()
            data = seq_nr_to_payload[last_sequence_number]                                  # we get its payload
            writing_mutex.release()

            g.seek(writing_offset)
            writing_offset += len(data)                                                     # we increment the writing offset
            logging.info("Wrote expected packet to file.")
            g.write(data)                                                                   # and we write to file

            last_sequence_number = ( last_sequence_number + len(data) +1) % (max_nr -1 )    # we update the sequence number of the next expected packet
        exit_mutex.acquire()
    exit_mutex.release()
    logging.info("Closing writing thread.")
    quit()



# this function waits for packages
def receive_payload(sock, emitter_address):
    global receiving_thread, write_thread, exit_mutex, exit
    global number_of_invalid_packets, number_of_packets_received
    
    receiving_thread = Thread(target= deal_with_received_packets, args=(sock,))             # we start the threads
    write_thread =  Thread(target = write_to_output, args=())
    window0_thread = Thread(target = window_0, args=(sock, emitter_address))

    receiving_thread.start()
    write_thread.start()
    window0_thread.start()
    
    logging.info("We start listening for payload packets.")
    sock.setblocking(0) 
    exit_mutex.acquire()
    while exit == False:
        exit_mutex.release()
        
        logging.info("Waiting for packets...")
        ready = select.select([sock], [], [], 1)                                            # to make sure that recvfrom is not blocking we use ready
        if ready[0]:
            data, emitter_address = sock.recvfrom(1400) 

            logging.info("Received a potential payload packet, appending it to queue.")
            number_of_packets_received +=1
            queue_mutex.acquire()
            queue.append((data, emitter_address))                                           # put the received packet into the queue
            queue_mutex.release()
            
        exit_mutex.acquire()
    exit_mutex.release()
    logging.info("Closing receive payload thread.")
    quit()

def setup_parser():
    global parser
    parser = ArgumentParser(usage=__file__ + ' '
                                             '-p/--port PORT'
                                             '-f/--fisier FILE_PATH',
                            description='Reliable UDP Receptor')

    parser.add_argument('-p', '--port',
                        dest='port',
                        default='10000',
                        help='Portul pe care sa porneasca receptorul pentru a primi mesaje')

    parser.add_argument('-f', '--fisier',
                        dest='fisier',
                        help='Calea catre fisierul in care se vor scrie octetii primiti')

def main():
    global exit, exit_mutex, parser, g, number_of_invalid_packets, number_of_packets_received
    global number_of_invalid_packets, number_of_packets_received
    
    setup_parser()

    args = vars(parser.parse_args())
    port = int(args['port'])
    file = args['fisier']                                       # file should be "/elocal/src/fisier.out"

    g = open(file, "wb")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, proto=socket.IPPROTO_UDP)

    address = '0.0.0.0'
    server_address = (address, port)

    sock.bind(server_address)
    logging.info("Server started on %s port %d", address, port)

    exit_mutex.acquire()
    while exit==False:
        exit_mutex.release()
        logging.info('Waiting for packets...')
        data, address = sock.recvfrom(4096)                                         # receives a packet
        logging.info("Received a packet, maybe we can establish a connection.")
        number_of_packets_received+=1

        if verifica_checksum(data):                                                 # checks its checksum

            if(receive_connection(sock, address, data)):                            # makes the connection with the emitter
                logging.info('We established a connection with "%s".', address)

                receive_payload(sock, address)                                      # starts receiving payload packets
                return

        else:
            number_of_invalid_packets+=1
            logging.info("Checksum is not valid, we ignored a packet before establishing a connection.")

        exit_mutex.acquire()
    exit_mutex.release()

    logging.info("Connection is closing.")
    sock.close()
    


if __name__ == '__main__':
    main()
