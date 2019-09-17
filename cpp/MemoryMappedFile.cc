/*
 * Copyright 2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
#include <iostream>
#include "MemoryMappedFile.h"

void MemoryMappedFile::mapAndSetOffset()
{ 
    // create this file before opening it.
    if ((_fd = open(_location.c_str(), O_RDWR)) < 0)
    {  
        cerr << "open error" << endl;
        exit(EXIT_FAILURE);
    }
    if ((fstat(_fd, &_st)) == -1)
    {
        cerr << "fstat error" << endl;
        exit(EXIT_FAILURE);
    }
    if (_st.st_size < _size)
    {
        cerr << "backed file is too small" << endl;
        cerr << "file size: " << _st.st_size << endl;
        cerr << "expected size: " << _size << endl;
        exit(EXIT_FAILURE); 
    }

    cout << "mem file size: " << _st.st_size << endl;

    _mapped = (char *) mmap(NULL, _st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
    if (_mapped == (void *)-1)
    {
        cerr << "mmap error" << endl;
        exit(EXIT_FAILURE);
    }

    close(_fd); // mappped memory is still available after file closed.
}