
import struct
import socket
import logging
import os

MAX_UINT32 = 0xFFFFFFFF
MAX_BITI_CHECKSUM = 16
MAX_SEGMENT = 1392            # the whole size of the payload will be 1400, if we add the reliable UDP header we added (8 bytes)
max_nr = 1<<16


def cut_sequence_number(sequence_number):                       # the aximum sequence number is (1<<16)-2

    return sequence_number % ((1<<MAX_BITI_CHECKSUM) -1)


def create_header_emitter(seq_nr, checksum, flags='S'):
    if flags=='S': 
        flags = 0b100
    if flags=='P': 
        flags = 0b010
    if flags=='F': 
        flags = 0b001

    spf_zero=flags<<13                                              # adding the padding because we hold spf_zero on 2 bytes

    seq_nr=cut_sequence_number(seq_nr)
    octeti = struct.pack('!LHH', seq_nr, int(checksum), spf_zero)   

    seq_nr, checksum, spf_zero = struct.unpack('!LHH', octeti)

    return octeti

def get_payload(octeti):                                            # from a emitter header, get payload
    return octeti[8:]


def parse_header_emitter(octeti):
    seq_nr, checksum, spf = struct.unpack('!LHH', octeti[:8])
    spf = spf>>13

    flags = ''
    if spf & 0b100:
        # inseamna ca am primit S
        flags = 'S'
    elif spf & 0b001:
        # inseamna ca am primit F
        flags = 'F'
    elif spf & 0b010:
        # inseamna ca am primit P
        flags = 'P'
    return (seq_nr, checksum, flags)


def create_header_receiver(ack_nr, checksum, window):
    octeti = struct.pack("!LHH", ack_nr, checksum, window)
    return octeti


def parse_header_receiver(octeti):
    ack_nr, checksum, window = struct.unpack("!LHH", octeti)
    return (ack_nr, checksum, window)


def read_segment(file_descriptor):
    mesaj = file_descriptor.read(MAX_SEGMENT)
    return mesaj

def make_segments(file_descriptor, window):                         # reads all input and returns it segmented in a list
    current_segments = []
    all_input = file_descriptor.read()
    for i in range(0, len(all_input), MAX_SEGMENT):
        current_segments.append(all_input[i:min(i+MAX_SEGMENT, len(all_input))])
    return current_segments

def calculeaza_checksum(octeti):
    global max_nr                           # this has value 1<<16
    checksum = 0
    numere = []

    if len(octeti) % 2 == 1:                # in case the packet has odd number of octets we add one more 
        octeti += bytes(1)

    for i in range(0, len(octeti), 2):      # we form 16 bit numbers from pairs of consecutive octets
        numar = octeti[i] << 8
        numar += octeti[i+1]
        numere.append(numar)

    sum=0
    for numar in numere:
        sum+=numar                          # we add these numbers to a sum
        sum1 = sum % max_nr                 # to deal with large amounts of numbers, we add the carry 
        sum2 = sum//max_nr                  # while adding the numbers
        sum = sum1+sum2
 
    checksum = max_nr - 1 - (sum)           # checksum is one's complement of this sum

    return checksum


def verifica_checksum(octeti):
    global max_nr
    sum = 0

    if len(octeti) % 2 == 1:
        octeti += bytes(1)

    for i in range(0, len(octeti), 2):
        numar = octeti[i] << 8              # we form 16 bit numbers from pairs of consecutive octets
        numar+=octeti[i+1]
        sum+=numar
        sum1 = sum % max_nr
        sum2 = sum//max_nr
        sum = sum1+sum2
        
                                            # by now this sum should be 65535, the largest number on 16 bits    
    sum+=1                                  # by adding 1, modulo (1<<16) we should get a 0
    sum = sum % max_nr

    if sum==0:
        return True
    return False

def calculate_smooth_rtt(last_smooth_rtt, last_interval):
    return 0.9*last_smooth_rtt + 0.1*last_interval

def calculate_smooth_variation(last_smooth_variation, last_interval, last_smooth_rtt):
    return 0.9*last_smooth_variation+0.1*abs(last_interval-last_smooth_variation)

def calculate_timeout(srtt, svar):
    return srtt + 4*svar
