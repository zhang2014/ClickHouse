#pragma once

#include <ostream>

namespace DB
{

void exitTip(std::ostream & out)
{
    time_t  now = time(NULL);
    struct tm tnow = *localtime(&now);
    std::string tz = tnow.tm_zone;
    out << "Local timezone: " << tz << std::endl;
}

}

