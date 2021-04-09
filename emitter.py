# emitator Reliable UDP
from helper import *
from argparse import ArgumentParser
import socket
import logging
import sys
import random
import _thread
from timer import Timer
import sys
import os
import time
from threading import Lock, Thread
import matplotlib.pyplot as plt

last_to_arrive = 0                              # holds the sequence number of the last packet to arrive with an acknowledgement number
last_smooth_rtt =1                              # holds the last smooth rtt calculated
last_variation = 0.1                            # holds the last avriation calculated
parser = 0                                      # just the parser we use to parse arguments given when running
segment = b""                                   # first segment we send
start = 0                                       # index of first packet in current window
next_to_send = 0                                # index of next packet to send
window = 0                                      # the last window we got from the receiver
send_timer = Timer(last_smooth_rtt)             # window timer
seq_nr_to_index = {}                            # a dictionary that maps sequence numbers of packets to their indexes in the packets list
packets = []                                    # the packets list
confirmed = []                                  # a list that marks packets for which we have received an acknowledgment
queue = []                                      # a queue in which we put received packets before we verify the checksum
leave_time = {}                                 # list that holds the time when every packet left
arrive_time = {}                                # a list that holds the time when every package arrived
connection_time = 0
finalize_time = 0
timeouts = [] 

number_of_sent_packets = 0 
number_of_packets_ack_for = 0
number_of_received_packets = 0
number_of_invalid_packets = 0

mutex_confirmed = Lock()                        # a mutex for the confirmed list
mutex_queue = Lock()                            # a mutex for the queue of packets received
mutex_last_to_arrive = Lock()                   # a mutex for the last_to_arrive variable
mutex_window = Lock()                           # a mutex for the window size
sequence_number = random.randint(0, 4294967295)     # the sequence number is initialised to a random 4 byte integer

logging.basicConfig(format = u'[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', level = logging.NOTSET)

# establishes the connection to the receiver
def connect(sock, receiver_address):
    global sequence_number, last_smooth_rtt, last_to_arrive, last_variation, mutex_last_to_arrive, connection_time
    global number_of_sent_packets, number_of_received_packets, number_of_packets_ack_for, number_of_invalid_packets

    flags = 'S'                                                                                 # 'S' means we want to start a new connection with receiver     
    header_octets_without_checksum = create_header_emitter(sequence_number, 0, flags)          # we first create the header with checksum set as 0

    checksum = calculeaza_checksum(header_octets_without_checksum)                              # for the first packet, there is no payload being sent

    header_octets_with_checksum = create_header_emitter(sequence_number, checksum, flags)      # now we include the checksum in the header

    message = header_octets_with_checksum                                                       # this is the message to be sent

    logging.info('Sending the message "%s" to %s', message, receiver_address[0])
    sent = sock.sendto(message, receiver_address)
    number_of_sent_packets+=1
    leave_time[sequence_number]=time.time()                                                     # we mark the leave time of this packet

    logging.info('Waiting for an answer...')
    try:

        data, server = sock.recvfrom(4096)                                                      # we're waiting for an answer for our connection package
        arrive_time[sequence_number]=time.time()                                                # we mark the time of the arrival
        number_of_received_packets+=1
        connection_time = time.time()

        mutex_last_to_arrive.acquire()
        last_to_arrive=sequence_number                                                          # and we also mark this package as the last one to arrive
        mutex_last_to_arrive.release()

        logging.info('Received content: "%s" de la "%s"', data, server)
    except socket.timeout as e:
        logging.info("Timeout while connecting, retrying...")
        return -1, -1

    if verifica_checksum(data) is False:                                                        # when we receive a apcket we verify the checksum
        number_of_invalid_packets+=1
        return -1, -1                                                                           # if it's invalid we ignore the packet

    # if the message is valid we return the acknowledgement number and the window

    ack_nr, checksum, w = parse_header_receiver(data)

    mutex_window.acquire()
    window=w
    mutex_window.release()

    logging.info("Connection created.")

    return ack_nr, window


# this thread receives packets and places them in the queue
def receiving(sock):
    global mutex, seq_nr_to_index, queue, mutex_last_to_arrive, last_to_arrive, last_variation, last_smooth_rtt
    global number_of_invalid_packets, number_of_packets_ack_for, number_of_received_packets, number_of_sent_packets
    
    logging.info("We start listening for acknowledgment packets.")
    while True:
        try:
            mutex_last_to_arrive.acquire()

            last_interval = arrive_time[last_to_arrive]-leave_time[last_to_arrive]                  # we get the last rtt

            srtt = calculate_smooth_rtt(last_smooth_rtt, last_interval)                             # calculate the smooth rtt
            svar = calculate_smooth_variation(last_variation, last_interval, last_smooth_rtt)       # calculate the smooth variance
            last_smooth_rtt=srtt                                                                    # save these values in the global variables
            last_variation=svar
            
            timeout = calculate_timeout(srtt, svar)
            logging.info(f"Timeout was set to: {timeout}")
            send_timer = Timer(timeout)                                          # we feed the srtt and svar to the function that calculates our timeout
                                                                                                    # and give it to the socket
            
            mutex_last_to_arrive.release()

            data, address = sock.recvfrom(1400)   
            number_of_received_packets +=1
            logging.info(f"Received a packet, appending it to queue.")
            mutex_queue.acquire()
            queue.append(data)                                                                      # and we add it to the queue
            mutex_queue.release()
        except socket.timeout as e:
            logging.info("Timeout la connect, retrying...")
            last_smooth_rtt = calculate_timeout(last_smooth_rtt, last_variation)                # if we get a timeout, we make sure to update smooth rtt
            


def parse_received_packets():
    global mutex_confirmed, mutex_queue, mutex_last_to_arrive, window
    global number_of_invalid_packets, number_of_packets_ack_for, number_of_received_packets, number_of_sent_packets
    logging.info("Started parsing received packets.")
    while True:
        packet = 0
        mutex_queue.acquire()
        if len(queue)>0:                                                                # if the queue is not empoty we fetch a packet from it
            packet = queue[0]
            queue.pop(0)
        mutex_queue.release()
        if packet!=0:
            ack_nr, checksum, w = parse_header_receiver(packet)                          

            if verifica_checksum(packet):                                               # if the packet has a valid checksum
                mutex_window.acquire()
                window=w                                                                    # we set the global window as the last window value we received from the receiver
                logging.info(f"New window is {window}.")
                mutex_window.release()
                
                if ack_nr != max_nr-1:                                                  
                    
                    if ack_nr ==0:                                                      # since we neet to subtract 1 from the ack_nr to get the initial seq_nr
                        ack_nr = max_nr -2                                              # and our seq_nr are modulo (max_nr-1), we need to meke sure that we address this case
                    seq_nr = ack_nr-1

                    index = seq_nr_to_index[seq_nr]                                     # we get the index of the packet for which we acknowledge from the seq_nr_to_index dictionary
                    number_of_packets_ack_for+=1
                    logging.info(f"Received acknowledgment for packet with index {index}.")

                    mutex_confirmed.acquire()
                    arrive_time[seq_nr]=time.time()                                     # we mark the arrival of this packet with its seq_nr
                    confirmed[index]=True
                    mutex_confirmed.release()

                    mutex_last_to_arrive.acquire()
                    last_to_arrive_mutex = seq_nr                                       # we also mark it as the last to arrive
                    mutex_last_to_arrive.release()
                else:                                                                   # if the ack_nr is max_nr -1 it means we got the special window_0 packet                                                                   # and we just need to update the window, not parse the packet 
                    number_of_invalid_packets+=1
                    logging.info(f"We received a special packet with window {w}\n")
            else:
                print(end="")
                logging.info(f"Checksum is not valid for packet.")

def finalize_connection(sock, receiver_address):
    global sequence_number, finalize_time, connection_time, timeouts
    global number_of_invalid_packets, number_of_packets_ack_for, number_of_received_packets, number_of_sent_packets

    flags = 'F'                                                                             # 'F' means we want to close the connection    
    header_octets_without_checksum = create_header_emitter(sequence_number, 0, flags)      # we first create the header without checksum

    checksum = calculeaza_checksum(header_octets_without_checksum)                          

    header_octets_with_checksum = create_header_emitter(sequence_number, checksum, flags)  # then we add the checksum

    message = header_octets_with_checksum

    logging.info(f'Sending message to finalize connection to {receiver_address}')              # and we send this final message to the receiver
    sent = sock.sendto(message, receiver_address)  
    number_of_sent_packets+=1   
    finalize_time=time.time()

    p1 = (number_of_invalid_packets/number_of_received_packets)*100
    p2 = (1 - number_of_packets_ack_for/ number_of_sent_packets)*100
    logging.info("'{0}' % corruption.".format(p1))
    logging.info("'{0}' % packet drop.".format(p2)) 
    logging.info("Program took {0} seconds.".format(finalize_time-connection_time)) 
    fig, ax = plt.subplots(nrows =1, ncols=1)
    plt.title("Adaptive timeout")
    plt.xlabel("time in seconds")
    plt.ylabel("timeout set for current window")
    Ox = []
    Oy = []
    for tuple in timeouts:
        Ox.append(tuple[1])
        Oy.append(tuple[0])
    ax.plot(Ox, Oy)
    fig.savefig('/elocal/src/graph.png')
    plt.close(fig)


def sliding_window(sock, receiver_address):
    global window, start, next_to_send, send_timer, packets, confirmed, seq_nr_to_index, last_smooth_rtt, last_to_arrive, last_variation
    global number_of_invalid_packets, number_of_packets_ack_for, number_of_received_packets, number_of_sent_packets, timeouts

    logging.info("Started sliding window.")

    _thread.start_new_thread(receiving, (sock, ))                             # we open the receiving thread                                      
    _thread.start_new_thread(parse_received_packets, ())                      # we open the thread that parces received packets


    while start<len(packets):
        mutex_window.acquire()
        w = window
        mutex_window.release()
        if w!= 0:
                                                                                                    # if the window size is not 0
            while next_to_send < min(start + w, len(packets)):                                      # we walk through the packets in the current window
                
                logging.info(f"Sending packet at index {next_to_send}")

                seq_nr = struct.unpack("!L", packets[next_to_send][0:4])[0]
                seq_nr = cut_sequence_number(seq_nr)

                leave_time[seq_nr]=time.time()                                                      # we mark the leave time of this packet
                send(packets[next_to_send], sock, receiver_address)                                  # send it 
                number_of_sent_packets+=1
                next_to_send+=1                                                                     # and increment the next_to_send pointer
            

            if not send_timer.running():                                                            # if the timer is not already running we start it
                # first we want to set the timeout for the receiving thread using the adaptive timout
                mutex_last_to_arrive.acquire()

                last_interval = arrive_time[last_to_arrive]-leave_time[last_to_arrive]                  # we get the last rtt

                srtt = calculate_smooth_rtt(last_smooth_rtt, last_interval)                             # calculate the smooth rtt
                svar = calculate_smooth_variation(last_variation, last_interval, last_smooth_rtt)       # calculate the smooth variance
                last_smooth_rtt=srtt                                                                    # save these values in the global variables
                last_variation=svar
                
                timeout = calculate_timeout(srtt, svar)
                timeouts.append((timeout, time.time()))
                logging.info(f"Timeout was set to: {timeout}")
                send_timer = Timer(timeout)                                          # we feed the srtt and svar to the function that calculates our timeout
                                                                                                        # and give it to the socket
                
                mutex_last_to_arrive.release()

                
                send_timer.start()
                logging.info("Starting timer.")

            mutex_confirmed.acquire()                                                   
            while send_timer.running() and not send_timer.timeout() and confirmed[start]==False:    # while the timer is still running and we haven't received 
                time.sleep(0.05)                                                                              # a confirmation for the first packet in the current window
                mutex_confirmed.release()
                mutex_confirmed.acquire()                                                           
            mutex_confirmed.release()

            if send_timer.timeout():                                                                # if there's a timeout we reset the next_to_send pointer
                logging.info("Window timeout.")
                send_timer.stop()
                last_smooth_rtt = calculate_timeout(last_smooth_rtt, last_variation)                # if we get a timeout, we make sure to update smooth rtt
                next_to_send = start
            else:
                mutex_confirmed.acquire()
                b = confirmed[start]                                                                # else we check once again that we received confirmation                                                                                    # for the first package of the current window
                mutex_confirmed.release()
                if b:
                    logging.info("Shifting window")
                    start+=1                                                                        # and shift out window
        else:
            print(end="")
            logging.info("Waiting to get another window or a special packet.")

    finalize_connection(sock, receiver_address)                                                      # after we finish the packets, we finalize the connection
            
# thransforms segments from input files into packets
def transform_packets(segmente):
    global sequence_number, packets, confirmed, seq_nr_to_index
    packets = []
    confirmed = []
    seq_nr_to_index = {}
    max_nr = 1 << 16

    logging.info("We start transforming the segments into packets.")

    for i in range(len(segmente)):
        segment = segmente[i]

        flags = 'P'                                                                                         # P means this package is part of the payload
        checksum = 0
        sequence_number = (sequence_number + len(segment)) % (max_nr-1)                                     # we make a sequence number based on the privious sequence number and 
                                                                                                            # the length of the payload
        header_octets_without_checksum = create_header_emitter(sequence_number, checksum, flags)           

        message = header_octets_without_checksum + segment

        checksum = calculeaza_checksum(message)

        header_octets_with_checksum = create_header_emitter(sequence_number, checksum, flags) + segment

        packet = header_octets_with_checksum
        packets.append(packet)                                                                              # we add the packet to the packets list
        mutex_confirmed.acquire()                                                                                                        
        confirmed.append(False)                                                                             # we mark the confirmation as false                                                                            
        mutex_confirmed.release()

        seq_nr_to_index[sequence_number] = i                                                                # we also add the sequence number to the dictionary that
                                                                                                            # maps the sequence numbers to the index in the packets

        leave_time[sequence_number] = 0 
        arrive_time[sequence_number] =0

        sequence_number = (sequence_number +1) % (max_nr-1)                                                 # we add one to this sequence number to prepare it for the
                                                                                                            # next packet
    
    logging.info(f"We finished transforming the segments ito packets, we have {len(packets)} packets.")
    return packets, confirmed, seq_nr_to_index


def send(packet, sock, receiver_address):
    sock.sendto(packet, receiver_address)

# sets up the parser
def setup_parser():
    global parser
    parser = ArgumentParser(usage=__file__ + ' '
                                             '-a/--adresa IP '
                                             '-p/--port PORT'
                                             '-f/--fisier FILE_PATH',
                            description='Reliable UDP Emitter')

    parser.add_argument('-a', '--adresa',
                        dest='adresa',
                        default='receptor',
                        help='Adresa IP a receptorului (IP-ul containerului, localhost sau altceva)')

    parser.add_argument('-p', '--port',
                        dest='port',
                        default='10000',
                        help='Portul pe care asculta receptorul pentru mesaje')

    parser.add_argument('-f', '--fisier',
                        dest='fisier',
                        help='Calea catre fisierul care urmeaza a fi trimis')





def main():
    global window, packets, seq_nr_to_index, confirmed
    global number_of_invalid_packets, number_of_packets_ack_for, number_of_received_packets, number_of_sent_packets

    setup_parser()                                                                                      # we setup the parser

    args = vars(parser.parse_args())                                                                    # we get the arguments

    receiver_ip = args['adresa']                                                                        # ip should be '198.8.0.2'
    receiver_port = int(args['port'])                                                                   
    file = args['fisier']                                                                               # the file that should be given is: '/elocal/src/fisier.bin'

    receiver_address = (receiver_ip, receiver_port)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, proto=socket.IPPROTO_UDP)
    sock.settimeout(3)

    ok = False                                                                                          # ok marks if we made a connection with the receiver or not
    while ok==False:
        ack_nr, window = connect(sock, receiver_address)
        ok=True
        if ack_nr==-1 or window==-1:
            ok=False

    sequence_number= cut_sequence_number(ack_nr)                                                        # the current sequence number is the acknowledge number we 
                                                                                                        # got from the initial connection

    
    f = open(file, 'rb')                                                                                # reading segments from input file

    current_segments = make_segments(f, window)                                                         # we make segments from the given file
    
    packets, confirmed, seq_nr_to_index = transform_packets(current_segments)                           # transforming those segments into packets ready to be sent

    sliding_window(sock, receiver_address)                                                              # we call our sliding window to start sending packets
        
    f.close()


if __name__ == '__main__':
    main()
